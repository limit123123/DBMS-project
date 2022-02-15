#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <file.h>

typedef struct Control
{
	page_t* frame;
	int64_t table_id;
	pagenum_t page_num;
	bool is_dirty;
	bool is_pinned;
	struct Control* next;
	struct Control* prev;

} Control;

typedef struct Bucket
{
	Control* control;
	struct Bucket* link;
} Bucket;

// head : most recently used 
// tail : least recently used
typedef struct
{
	Control* head;
	Control* tail;
} LRU_list;


void init_lru_list(int buf_num);
void update_lru_list(Control* control);


void init_hash_table();
int hash_func(pagenum_t page_num);
void add_hash_table(Control* control);
void del_hash_table(Control* control);
Control* search_hash_table(uint64_t table_id, pagenum_t key);
void free_hash_table();


int buf_init_db(int buf_num);
pagenum_t buf_alloc_page(int64_t table_id);
void buf_free_page(int64_t table_id, pagenum_t page_num);
void buf_read_page(int64_t table_id, pagenum_t page_num, page_t* dest);
void buf_pin_out(int64_t table_id, pagenum_t page_num);
void buf_write_page(int64_t table_id, pagenum_t page_num, page_t* src);
int buf_shutdown_db();

#endif /*__BUFFER_H__*/
