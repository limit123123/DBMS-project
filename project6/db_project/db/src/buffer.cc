#include "buffer.h"
#include "file.h"
#include "trx.h"
#include "log.h"
#include <stdlib.h>
#include <iostream>
#include <cstring>
#include <pthread.h>

using namespace std;

extern int64_t curr_LSN;
extern pthread_mutex_t log_manager_latch;

Control* control_block;
page_t* frame_block;

pthread_mutex_t buf_pool_latch = PTHREAD_MUTEX_INITIALIZER;

// 2^10 ~ 2^11 사이의 소수
const int TABLE_SIZE = 1543;
Bucket** hash_table;
LRU_list lru_list;


void init_lru_list(int num_buf)
{
	if ( num_buf < 4 ) return;

	lru_list.head = &control_block[0];
	lru_list.tail = &control_block[num_buf-1];

	Control* temp = lru_list.head;
	for( int i = 1; i < num_buf; i++ )
	{
		temp->next = &control_block[i];
		temp = temp->next;
	}

	temp = lru_list.tail;
	for ( int i = num_buf - 2; i >= 0; i-- )
	{
		temp->prev = &control_block[i];
		temp = temp->prev;
	}

	control_block[0].prev = NULL;
	control_block[num_buf-1].next = NULL;
}

void update_lru_list(Control* control)
{
	if ( control == lru_list.head ) return;

	if ( control == lru_list.tail ) 
	{
		control->prev->next = NULL;
		lru_list.tail = control->prev;
	}
	else
	{
		control->prev->next = control->next;
		control->next->prev = control->prev;
	}
	
   	// insert behind head node
	control->next = lru_list.head;
	lru_list.head->prev = control;
	control->prev = NULL;
	lru_list.head = control;
}

void init_hash_table()
{
	hash_table = (Bucket**)malloc(sizeof(Bucket*) * TABLE_SIZE);
	for( int i = 0; i < TABLE_SIZE; i++ )
		hash_table[i] = NULL;
}

int hash_func(pagenum_t page_num)
{
	return page_num % TABLE_SIZE;
}

void add_hash_table(Control* control)
{
	int hash_val = hash_func(control->page_num);
	Bucket* bucket = hash_table[hash_val];

	Bucket* bptr = (Bucket*)malloc(sizeof(Bucket));
	if ( bptr == NULL ) return;

	// add in front of the list.
	bptr->control = control;
	bptr->link = hash_table[hash_val];
	hash_table[hash_val] = bptr;
}

void del_hash_table(Control* control)
{
	int hash_val = hash_func(control->page_num);
	Bucket* bucket = hash_table[hash_val];

	if ( bucket == NULL ) return;

	if ( bucket->control == control )
	{
		hash_table[hash_val] = bucket->link;
		if ( bucket != NULL ) free(bucket);
		return;
	}

	while( bucket->link )
	{
		if ( bucket->link->control == control )
		{
			Bucket* temp = bucket->link;
			bucket->link = bucket->link->link;
			free(temp);
			return;
		}
		bucket = bucket->link;
	}
}

Control* search_hash_table(int64_t table_id, pagenum_t key)
{
	int hash_val = hash_func(key);
	Bucket* bucket;

	for( bucket = hash_table[hash_val]; bucket; bucket = bucket->link )
	{
		if ( bucket->control->table_id == table_id && bucket->control->page_num == key )
			return bucket->control;
	}

	return NULL;
}

void free_hash_table()
{
	Bucket* mov_temp = NULL, *del_temp = NULL;
	
	for( int i = 0; i < TABLE_SIZE; i++ )
	{
		if ( hash_table[i] != NULL )
		{
			mov_temp = hash_table[i];
			while( mov_temp != NULL )
			{
				del_temp = mov_temp;
				mov_temp = mov_temp->link;
				free(del_temp);
			}
		}
	}
	free(hash_table);
}


int buf_init_db(int num_buf)
{
	if ( num_buf < 4 ) return -1;

	pthread_mutex_lock(&buf_pool_latch);

	// Buffer pool initialize.
	control_block = (Control*)malloc(sizeof(Control) * num_buf);
	if ( control_block == NULL ) return -1;

	frame_block = (page_t*)malloc(sizeof(page_t) * num_buf);
	if ( frame_block == NULL ) return -1;

	for( int i = 0; i < num_buf; i++ )
	{
		control_block[i].frame = &frame_block[i];
		control_block[i].table_id = -1;
		control_block[i].page_num = 0;
		control_block[i].is_dirty = false;
		if (pthread_mutex_init(&control_block[i].page_latch, NULL) != 0) return -1;
		control_block[i].next = NULL;
		control_block[i].prev = NULL;
	}
	
	// Hash table initialize
	init_hash_table();

	// LRU list initialize
	init_lru_list(num_buf);

	pthread_mutex_unlock(&buf_pool_latch);
	return 0;
}

void buf_read_page(int64_t table_id, pagenum_t page_num, page_t* dest)
{
	pthread_mutex_lock(&buf_pool_latch);

	Control* control = search_hash_table(table_id, page_num);
	
	// cache miss
	if ( control == NULL )
	{
		// Pick the LRU control(frame) block.
		control = lru_list.tail;
		
		// Find unpinned(= unlocked) control(frame) block 
		while (pthread_mutex_trylock(&control->page_latch) != 0)
			control = control->prev;

		del_hash_table(control);
		if (control->is_dirty)
		{
			pthread_mutex_lock(&log_manager_latch);
			log_buf_force();
			pthread_mutex_unlock(&log_manager_latch);
			file_write_page(control->table_id, control->page_num, control->frame);
		}

		file_read_page(table_id, page_num, control->frame);
		control->table_id = table_id;
		control->page_num = page_num;
		control->is_dirty = false;
		
		add_hash_table(control);

		update_lru_list(control); 
		memcpy(dest, control->frame, PAGE_SIZE);
	
		pthread_mutex_unlock(&buf_pool_latch);

		return;
	}
	else
	{
		// Acquire the page latch.( is_pinned = true )
		pthread_mutex_lock(&control->page_latch);

		// LRU list is protected by the buffer manager latch.
		update_lru_list(control); 
		memcpy(dest, control->frame, PAGE_SIZE);

		pthread_mutex_unlock(&buf_pool_latch);
		
		return;
	}
}

// if page read is only for reading not updating, 
// then read and pull out pin right away.
void buf_pin_out(int64_t table_id, pagenum_t page_num)
{
	//Control* control = search_hash_table(table_id, page_num);
	//if ( control != NULL ) control->is_pinned = false;

}

void buf_page_latch_acquire(int64_t table_id, pagenum_t page_num)
{
	pthread_mutex_lock(&buf_pool_latch);

	Control* control = search_hash_table(table_id, page_num);
	if (control != NULL) pthread_mutex_lock(&control->page_latch);

	pthread_mutex_unlock(&buf_pool_latch);
}

void buf_page_latch_release(int64_t table_id, pagenum_t page_num)
{
	Control* control = search_hash_table(table_id, page_num);
	if (control != NULL) pthread_mutex_unlock(&control->page_latch);
}

void buf_write_page(int64_t table_id, pagenum_t page_num, page_t* src)
{

	// The frame to be written is locked now. So, the page was not evicted.
	// So, it must be in hash table.
	Control* control = search_hash_table(table_id, page_num);
	memcpy(control->frame, src, PAGE_SIZE);
	control->is_dirty = true;

	update_lru_list(control); 

	// fflush header page right away.
	if (page_num == 0)
	{
		log_buf_force();
		file_write_page(control->table_id, 0, control->frame);	
		control->is_dirty = false;
	}

	pthread_mutex_unlock(&control->page_latch); //control->is_pinned = false;
}

pagenum_t buf_alloc_page(int64_t table_id)
{
	pthread_mutex_lock(&buf_pool_latch);

	Control* control = lru_list.tail;
	
	
	// Find unpinned(= unlocked) control(frame) block 
	while (pthread_mutex_trylock(&control->page_latch) != 0)
		control = control->prev;
	// the page in frame will be evicted.
	pthread_mutex_unlock(&control->page_latch);
	
   	
	del_hash_table(control);
	// fflush
   	if (control->is_dirty)
	{
		pthread_mutex_lock(&log_manager_latch);
		log_buf_force();
		pthread_mutex_unlock(&log_manager_latch);

    	file_write_page(control->table_id, control->page_num, control->frame);
	}

	// file_alloc_page -> meta data of header page in disk is changed.
	pagenum_t new_page_num = file_alloc_page(table_id);
	//  If there is a header page in the current buffer pool, match the contents of the header page on the disk.
	Control* header_control = search_hash_table(table_id, 0);
	if ( header_control != NULL )
		file_read_page(table_id, 0, header_control->frame);


	//memset(control->frame, 0x00, PAGE_SIZE);
	control->table_id = table_id;
	control->page_num = new_page_num;
	control->is_dirty = false;

 	add_hash_table(control);
     
	update_lru_list(control);

	pthread_mutex_unlock(&buf_pool_latch);

	return new_page_num;
}

void buf_free_page(int64_t table_id, pagenum_t page_num)
{
	pthread_mutex_lock(&buf_pool_latch);

	Control* control = search_hash_table(table_id, page_num);
	
	del_hash_table(control);

	//memset(control->frame, 0x00, PAGE_SIZE);
	control->table_id = -1;
	control->page_num = 0;
	control->is_dirty = false;

	pthread_mutex_unlock(&control->page_latch); //control->is_pinned = false;

	update_lru_list(control);
	
	// file_free_page -> meta data of header page in disk is changed.
	file_free_page(table_id, page_num);
	//  If there is a header page in the current buffer pool, match the contents of the header page on the disk.
	Control* header_control = search_hash_table(table_id, 0);
	if ( header_control != NULL )
		file_read_page(table_id, 0, control->frame);

	pthread_mutex_unlock(&buf_pool_latch);
}

int buf_shutdown_db()
{
	Control* temp = lru_list.head;
	while( temp )
	{
		if ( temp->is_dirty ) 
			file_write_page(temp->table_id, temp->page_num, temp->frame);
		
		temp = temp->next;
	}

	pthread_mutex_lock(&buf_pool_latch);
	free_hash_table();
	pthread_mutex_unlock(&buf_pool_latch);

	free(control_block);
	free(frame_block);
	return 0;
}
