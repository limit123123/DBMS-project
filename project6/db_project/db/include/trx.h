#ifndef __TRX_H__
#define __TRX_H__

#include "page.h"
#include "bpt.h"
#include "log.h"
#include "buffer.h"
#include <stdint.h>
#include <list>
#include <queue>
#include <stack>
#include <map>
#include <utility>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef struct lock_t lock_t;
typedef struct lock_Bucket lock_Bucket;

// LOCK MODE
#define SHARED 0
#define EXCLUSIVE 1

// LOCK STATE
#define ACQUIRED 2
#define WAITING 3

// TRX STATE
#define WORKING 4
#define SLEEPING 5

// RESULT OF LOCK ACQUIRE
#define IS_DEADLOCK 1024
#define PASS_WITHOUT_PAGE 1025
#define PASS_WITH_PAGE 1026

// CHECK CONFLICT
#define IS_CONFLICT_LOCK 1030
#define IMPLICIT_ALIVE_CONVERTED 1031
#define IMPLICIT_ALIVE_NOT_CONVERTED 1032
#define NO_CONFLICT 1033
#define CONFLICT_IN_COMPRESSION 1034

#define MAX_NUM_OF_SLOT 64
#define MAX_SIZE_OF_VALUE 108

struct lock_t 
{
	lock_t* prev;
	lock_t* next;
	lock_Bucket* p_sentinel;
	pthread_cond_t cond;
	int64_t record_id;
	int lock_mode;
	lock_t* trx_next_lock;
	int owner_trx_id;
	
	bool bitmap[MAX_NUM_OF_SLOT];
	int lock_state;
	lock_t* same_rid_prev;
	lock_t* same_rid_next; 
};


struct lock_Bucket
{
	int64_t table_id;
	pagenum_t page_id;
	lock_t* tail;
	lock_t* head;

	lock_Bucket* prev;
	lock_Bucket* next;
};

typedef struct old_log_t
{
	int64_t table_id;
	pagenum_t page_id;
	int64_t key;
	char old_val[MAX_SIZE_OF_VALUE];
	uint16_t old_val_size;
}old_log_t;


typedef struct trx_t
{
	int trx_id;
	lock_t* head_trx_lock_list; 
	int trx_state;

	int64_t last_LSN; 
	std::stack<old_log_t*> old_logs;
	bool visited;  
}trx_t;


// API for Transaction table
int trx_begin();
int trx_commit(int trx_id);
void trx_write_log(int64_t table_id, pagenum_t page_id, int64_t key, char* old_val, uint64_t old_val_size, int trx_id);
int trx_roll_back_record(std::stack<old_log_t*> old_logs);
int trx_abort(int trx_id);


// API for lock hash table
uint64_t hash_func(int64_t table_id, pagenum_t page_id);
lock_Bucket* add_lock_table(int64_t table_id, pagenum_t page_id);
lock_Bucket* search_lock_table(int64_t table_id, pagenum_t page_id);
int init_lock_table();
void free_lock_table();

lock_t* make_new_lock_object(lock_Bucket* bucket, int64_t key, int trx_id, int lock_mode);
bool is_my_trx_already_acquired_same_or_stronger_lock_type(lock_Bucket* bucket, int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode, int record_index, page_t* page);

int lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode, int record_index, page_t* page);
bool is_conflict_1(lock_t* pred_lock_obj, lock_t* succ_lock_obj);
bool is_conflict_2(lock_t* pred_lock_obj, int trx_id, int64_t record_id, int lock_mode);
std::pair<int,int> is_conflict_lock_in_record_lock_list(lock_t* pred_same_rid_lock_obj, int64_t table_id, pagenum_t page_id, int trx_id, int64_t record_id, int lock_mode, int record_index, page_t* page);
bool check_deadlock(lock_t* new_lock_obj, int record_index);
bool is_cycle(int suspicious_trx_id, int new_trx_id);


int lock_release(lock_t* lock_obj);
void update_bucket_lock_list_after_release(lock_Bucket* bucket, lock_t* lock_obj);
void update_record_lock_list_after_release(lock_t* lock_obj);
bool is_acquired_lock_in_record_lock_list(lock_t* lock_obj);


#endif /* __TRX_H__ */
