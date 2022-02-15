#ifndef __DISK_BASED_BPT_H__
#define __DISK_BASED_BPT_H__

#include <buffer.h>

#define MIN_SIZE_OF_VALUE 50
#define MAX_SIZE_OF_VALUE 112
#define MIN_NUM_OF_SLOT 32
#define MAX_NUM_OF_SLOT 64
#define INTERNAL_ORDER 249
#define SPLIT_THRESHOLD 1984
#define THRESHOLD 2500
#define SLOT_SIZE 12
#define SLOT_STARTING_POINT 128 

int64_t open_table (char *pathname);

int db_insert (int64_t table_id, int64_t key, char * value, uint16_t val_size);

int db_find (int64_t table_id, int64_t key, char * ret_val, uint16_t * val_size);

int db_delete (int64_t table_id, int64_t key);

int init_db (int num_buf);

int shutdown_db();

// find the leaf_page that maybe has key.
pagenum_t db_find_leaf (int64_t table_id, int64_t key);

// First insertion
int db_start_new_tree(int64_t table_id, int64_t key, char* value, uint64_t val_size);
int db_insert_into_leaf(int64_t table_id, Leaf_page* p_leaf_page, pagenum_t leaf_pagenum, int64_t key, char* value, uint16_t val_size);
int db_insert_into_leaf_after_splitting(int64_t table_id, Leaf_page* p_old_leaf_page, pagenum_t leaf_pagenum, int64_t key, char* value, uint16_t val_size);
int db_insert_into_parent(int64_t table_id, pagenum_t left_pagenum, uint64_t key, pagenum_t right_pagenum, pagenum_t parent_pagenum);
int db_insert_into_new_root(int64_t table_id, pagenum_t left_pagenum, uint64_t key, pagenum_t right_pagenum);
int db_insert_into_internal_after_splitting(int64_t table_id, Internal_page* p_old_page, pagenum_t old_pagenum, int left_index, int64_t key, pagenum_t right_pagenum);


int db_cut( int length );
int db_delete_entry(int64_t table_id, pagenum_t pagenum, uint64_t key);
pagenum_t db_remove_entry_from_page(int64_t table_id, pagenum_t pagenum, uint64_t key);
int db_adjust_root(int64_t table_id, pagenum_t root_pagenum);
int get_neighbor_index(int64_t table_id, pagenum_t del_pagenum, Internal_page* p_parent_page);
int merge_leaf_pages(int64_t table_id, pagenum_t leaf_pagenum, pagenum_t neighbor_pagenum, int k_prime);
int merge_internal_pages(int64_t table_id, pagenum_t internal_pagenum, pagenum_t neighbor_pagenum,int k_prime);
int redistribute_leaf_pages(int64_t table_id, pagenum_t leaf_pagenum, pagenum_t neighbor_pagenum,int k_prime_index);
int redistribute_internal_pages(int64_t table_id, pagenum_t internal_pagenum, pagenum_t neighbor_pagenum, int k_prime_index, int k_prime, int min_keys);

#endif /*__DISK_BASED_BPT_H__*/
