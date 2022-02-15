#include "lock_table.h"
#include <pthread.h>
#include <stdlib.h>

typedef struct Bucket
{
	int64_t table_id;
	int64_t record_id;
	lock_t* tail;
	lock_t* head;
	struct Bucket* link;
}Bucket;


struct lock_t 
{
	lock_t* prev;
	lock_t* next;
	Bucket* p_sentinel;
	pthread_cond_t cond;	
};

typedef struct lock_t lock_t;

const int64_t TABLE_SIZE = 6151;
Bucket* lock_table[TABLE_SIZE];

pthread_mutex_t lock_table_latch;


int64_t hash_func(int64_t table_id, int64_t key)
{
    return (table_id%TABLE_SIZE + key%TABLE_SIZE)%TABLE_SIZE;
}

Bucket* add_lock_table(int64_t table_id, int64_t key)
{
    int64_t hash_val = hash_func(table_id, key);

    Bucket* bucket = (Bucket*)malloc(sizeof(Bucket));
    if ( bucket == NULL ) return NULL;

    bucket->table_id = table_id;
	bucket->record_id = key;
	bucket->tail = NULL;
	bucket->head = NULL;
    bucket->link = lock_table[hash_val];
    lock_table[hash_val] = bucket;

	return bucket;
}

Bucket* search_lock_table(int64_t table_id, int64_t key )
{
    int64_t hash_val = hash_func(table_id, key);
    Bucket* bucket;

    for( bucket = lock_table[hash_val]; bucket; bucket = bucket->link )
    {
        if ( bucket->table_id == table_id && bucket->record_id == key )
            return bucket;
    }

    return NULL;
}

int init_lock_table() 
{
	 for( int64_t i = 0; i < TABLE_SIZE; i++ )
		 lock_table[i] = NULL;

	 return pthread_mutex_init(&lock_table_latch, NULL);
}

lock_t* lock_acquire(int64_t table_id, int64_t key) 
{
	if (pthread_mutex_lock(&lock_table_latch) != 0) return NULL;

	Bucket* bucket = search_lock_table(table_id, key);	
	if ( bucket == NULL ) bucket = add_lock_table(table_id, key);

	lock_t* lock_obj = (lock_t*)malloc(sizeof(lock_t));
	if ( lock_obj == NULL ) return NULL;
	
	lock_obj->p_sentinel = bucket;
	if (pthread_cond_init(&lock_obj->cond, NULL) != 0) return NULL;

	if ( bucket->head == NULL )
	{
		bucket->head = lock_obj;
		bucket->tail = lock_obj;
		lock_obj->prev = NULL;
		lock_obj->next = NULL;
	}
	else 
	{
		bucket->tail->next = lock_obj;
		lock_obj->prev = bucket->tail;
		bucket->tail = lock_obj;
		lock_obj->next = NULL;

		if (pthread_cond_wait(&lock_obj->cond, &lock_table_latch) != 0) return NULL;
	}

	if (pthread_mutex_unlock(&lock_table_latch) != 0) return NULL;
	
	return lock_obj;
}

int lock_release(lock_t* lock_obj) 
{
	if (pthread_mutex_lock(&lock_table_latch) != 0) return -1;
	
	Bucket* bucket = lock_obj->p_sentinel;

	if ( bucket->tail == lock_obj )
	{
		bucket->head = NULL;
		bucket->tail = NULL;	
	}
	else
	{
		bucket->head = lock_obj->next;
		lock_obj->next->prev = NULL;
		if (pthread_cond_signal(&bucket->head->cond) != 0) return -1;
	}
	free(lock_obj);
	if (pthread_mutex_unlock(&lock_table_latch) != 0) return -1;
	
	return 0;
}
