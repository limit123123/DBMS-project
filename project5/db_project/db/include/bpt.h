#ifndef __BPT_H__
#define __BPT_H__

#include <buffer.h>
#include <trx.h>

#define MIN_SIZE_OF_VALUE 46
#define MAX_SIZE_OF_VALUE 108
#define MIN_NUM_OF_SLOT 32
#define MAX_NUM_OF_SLOT 64
#define INTERNAL_ORDER 249
#define SPLIT_THRESHOLD 1984
#define THRESHOLD 2500
#define SLOT_SIZE 16
#define SLOT_STARTING_POINT 128 


// APIs for DB setting
int64_t open_table(char *pathname);
int init_db(int num_buf);
int shutdown_db();

// API for B+ tree finding
int db_find_without_trx(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size);
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id);
pagenum_t db_find_leaf(int64_t table_id, int64_t key);

// APIs for B+ tree insertion
int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size);
int db_start_new_tree(int64_t table_id, int64_t key, char* value, uint64_t val_size);
int db_insert_into_leaf(int64_t table_id, Leaf_page* p_leaf_page, pagenum_t leaf_pagenum, int64_t key, char* value, uint16_t val_size);
int db_insert_into_leaf_after_splitting(int64_t table_id, Leaf_page* p_old_leaf_page, pagenum_t leaf_pagenum, int64_t key, char* value, uint16_t val_size);
int db_insert_into_parent(int64_t table_id, pagenum_t left_pagenum, uint64_t key, pagenum_t right_pagenum, pagenum_t parent_pagenum);
int db_insert_into_new_root(int64_t table_id, pagenum_t left_pagenum, uint64_t key, pagenum_t right_pagenum);
int db_insert_into_internal_after_splitting(int64_t table_id, Internal_page* p_old_page, pagenum_t old_pagenum, int left_index, int64_t key, pagenum_t right_pagenum);


// API for B+ tree update
int db_update(int64_t table_id, int64_t key, char* value, uint16_t new_val_size, uint16_t* old_val_size, int trx_id);


// APIs for B+ tree deletion
int db_delete (int64_t table_id, int64_t key);
int db_cut(int length);
int db_delete_entry(int64_t table_id, pagenum_t pagenum, uint64_t key);
pagenum_t db_remove_entry_from_page(int64_t table_id, pagenum_t pagenum, uint64_t key);
int db_adjust_root(int64_t table_id, pagenum_t root_pagenum);
int get_neighbor_index(int64_t table_id, pagenum_t del_pagenum, Internal_page* p_parent_page);
int merge_leaf_pages(int64_t table_id, pagenum_t leaf_pagenum, pagenum_t neighbor_pagenum, int k_prime);
int merge_internal_pages(int64_t table_id, pagenum_t internal_pagenum, pagenum_t neighbor_pagenum,int k_prime);
int redistribute_leaf_pages(int64_t table_id, pagenum_t leaf_pagenum, pagenum_t neighbor_pagenum,int k_prime_index);
int redistribute_internal_pages(int64_t table_id, pagenum_t internal_pagenum, pagenum_t neighbor_pagenum, int k_prime_index, int k_prime, int min_keys);

#endif /*__BPT_H__*/
