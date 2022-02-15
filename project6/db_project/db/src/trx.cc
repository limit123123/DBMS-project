#include "trx.h"
#include "log.h"
#include <iostream>

using namespace std;

const int64_t LOCK_TABLE_SIZE = 1543;
lock_Bucket* lock_table[LOCK_TABLE_SIZE];
pthread_mutex_t lock_manager_latch;

std::map<int, trx_t> trx_table;
pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
int TRX_ID = 1;

extern pthread_mutex_t log_manager_latch;
extern int64_t curr_LSN;

uint64_t hash_func(int64_t table_id, pagenum_t page_id)
{
    return (table_id%LOCK_TABLE_SIZE + page_id%LOCK_TABLE_SIZE)%LOCK_TABLE_SIZE;
}

lock_Bucket* add_lock_table(int64_t table_id, pagenum_t page_id)
{
    uint64_t hash_val = hash_func(table_id, page_id);

    lock_Bucket* new_bucket = (lock_Bucket*)malloc(sizeof(lock_Bucket));
    if (new_bucket == NULL) return NULL;

    new_bucket->table_id = table_id;
	new_bucket->page_id = page_id;
	
	// Initialize lock list in Bucket
	new_bucket->tail = NULL;
	new_bucket->head = NULL;

	// Link the bucket list.
	if (lock_table[hash_val] == NULL) 
	{
		lock_table[hash_val] = new_bucket;
		new_bucket->prev = NULL;
		new_bucket->next = NULL;
	}
	else
	{
		new_bucket->next = lock_table[hash_val];
		lock_table[hash_val]->prev = new_bucket;
		lock_table[hash_val] = new_bucket;
		new_bucket->prev = NULL;
	}
	
	return new_bucket;
}

lock_Bucket* search_lock_table(int64_t table_id, pagenum_t page_id)
{
    uint64_t hash_val = hash_func(table_id, page_id);

    for (lock_Bucket* bucket = lock_table[hash_val]; bucket; bucket = bucket->next) 
    {
        if (bucket->table_id == table_id && bucket->page_id == page_id)
            return bucket;
    }

    return NULL;
}

int init_lock_table(void) 
{
	if (pthread_mutex_init(&lock_manager_latch, NULL) != 0) return -1;
	return 0;
}

void free_lock_table()
{
	lock_Bucket* mov_temp = NULL, *del_temp = NULL;
	
	for (int i = 0; i < LOCK_TABLE_SIZE; i++)
	{
		if (lock_table[i] != NULL)
		{
			mov_temp = lock_table[i];
			while (mov_temp != NULL)
			{
				del_temp = mov_temp;
				mov_temp = mov_temp->next;
				free(del_temp);
			}
		}
	}
}


lock_t* make_new_lock_object(lock_Bucket* bucket, int64_t key, int trx_id, int lock_mode)
{
	lock_t* new_lock_obj = (lock_t*)malloc(sizeof(lock_t));

	new_lock_obj->prev = NULL;
	new_lock_obj->next = NULL;
	new_lock_obj->p_sentinel = bucket;

	pthread_cond_init(&(new_lock_obj->cond), NULL);

	new_lock_obj->record_id = key;
	new_lock_obj->lock_mode = lock_mode;
	new_lock_obj->owner_trx_id = trx_id;
	new_lock_obj->trx_next_lock = NULL;
	
	trx_t& trx_obj = trx_table[trx_id];
	if (trx_obj.head_trx_lock_list == NULL)
	{
		trx_obj.head_trx_lock_list = new_lock_obj;
	}
	else
	{
		new_lock_obj->trx_next_lock = trx_obj.head_trx_lock_list;
		trx_obj.head_trx_lock_list = new_lock_obj;
	}

	new_lock_obj->same_rid_prev = NULL;
	new_lock_obj->same_rid_next = NULL;
	
	/* This code is for lock compression */
	//memset(new_lock_obj->bitmap, 0, MAX_NUM_OF_SLOT);

	return new_lock_obj;
}


bool is_my_trx_already_acquired_same_or_stronger_lock_type(lock_Bucket* bucket, int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode, int record_index, page_t* page)
{
	char delivery[PAGE_SIZE];
	Slot slot;

	//buf_read_page(table_id, page_id, (page_t*)delivery);
	//buf_page_latch_release(table_id, page_id);
	
	memcpy(delivery, page, PAGE_SIZE);

	memcpy(&slot, delivery+(SLOT_STARTING_POINT + SLOT_SIZE*record_index), SLOT_SIZE);
	int record_trx_id = slot.trx_id;

	// Case : A transaction has already implicit lock(X lock) on that record.
	if (record_trx_id == trx_id) return true;


	/* This code is for lock compression */
	/*
	// Case : lock compression check
	lock_t* temp_lock_obj = bucket->head;

	if (lock_mode == SHARED)
	{
		while (temp_lock_obj != NULL)
		{
			if (temp_lock_obj->owner_trx_id == trx_id && temp_lock_obj->lock_mode == SHARED && temp_lock_obj->lock_state == ACQUIRED)
			{
				if (temp_lock_obj->bitmap[record_index] == true) return true;
			}
			temp_lock_obj = temp_lock_obj->next;
		}
	}
	*/

	// Case : A transaction has already same or stronger lock in record lock list.
	lock_t* temp_lock_obj = bucket->head;

	// First, find the lock object with same record id (to seek record lock list)
	while (temp_lock_obj != NULL)
	{
		if (temp_lock_obj->record_id == key) break;
		temp_lock_obj = temp_lock_obj->next;

	}
	if (temp_lock_obj == NULL) return false;
	
	// Second, find whether my trx's already acquired same or stronger lock type or not.
	while (temp_lock_obj != NULL)
	{
		if (temp_lock_obj->owner_trx_id == trx_id && (temp_lock_obj->lock_mode >= lock_mode) && temp_lock_obj->lock_state == ACQUIRED)
			return true;
		
		temp_lock_obj = temp_lock_obj->same_rid_next;
	}
	if (temp_lock_obj == NULL) return false;
}



// return type : lock_t* -> int ( SUCCESS / IS_DEADLOCK )
// parameter add : int record_index
int lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode, int record_index, page_t* page) 
{
	pthread_mutex_lock(&lock_manager_latch);	

	lock_Bucket* bucket = search_lock_table(table_id, page_id);	

	// Case : No Bucket
	if (bucket == NULL) 
	{	
		bucket = add_lock_table(table_id, page_id);

		if (lock_mode == SHARED)
		{
			lock_t* new_lock_obj = make_new_lock_object(bucket, key, trx_id, lock_mode);

			// Update Bucket lock list
			bucket->head = new_lock_obj;
			bucket->tail = new_lock_obj;
			new_lock_obj->lock_state = ACQUIRED;
	

			// lock compression
			//new_lock_obj->bitmap[record_index] = true;

			pthread_mutex_unlock(&lock_manager_latch);
			return PASS_WITH_PAGE;
		}
		else if (lock_mode == EXCLUSIVE)
		{
			// Do implicit locking, Don't need to make lock object.
			pthread_mutex_unlock(&lock_manager_latch);
			return PASS_WITH_PAGE;
		}
	}
	// Case : There is a Bucket
	else 
	{
		if (is_my_trx_already_acquired_same_or_stronger_lock_type(bucket, table_id, page_id, key, trx_id, lock_mode, record_index, page))
		{

			pthread_mutex_unlock(&lock_manager_latch);
			return PASS_WITH_PAGE;
		}


		// Find first lock object with same record id in bucket lock list.
		lock_t* temp_lock_obj = bucket->head;	
		while (temp_lock_obj != NULL)
		{
			if (temp_lock_obj->record_id == key) break;
			temp_lock_obj = temp_lock_obj->next;
		}

		// Find last lock object with same record id in bucket lock list.
		lock_t* pred_same_rid_lock_obj = temp_lock_obj;
		while (pred_same_rid_lock_obj != NULL)
		{
			if (pred_same_rid_lock_obj->same_rid_next == NULL) break;	
			pred_same_rid_lock_obj = pred_same_rid_lock_obj->same_rid_next;
		}


		pair<int,int> result_conflict_check = is_conflict_lock_in_record_lock_list(pred_same_rid_lock_obj, table_id, page_id, trx_id, key, lock_mode, record_index, page); 

		if (result_conflict_check.first == NO_CONFLICT)
		{

			if (lock_mode == SHARED)
			{

				/* This code is for lock compression*/
				/*
				// Case : There is already my acquried S lock in bucket lock list
				while (temp_lock_obj != NULL)
				{
					if (temp_lock_obj->owner_trx_id == trx_id && temp_lock_obj->lock_mode == SHARED && temp_lock_obj->lock_state == ACQUIRED)
					{
						temp_lock_obj->bitmap[record_index] == true;
						pthread_mutex_unlock(&lock_manager_latch);
						return SUCCESS;
					}
					temp_lock_obj = temp_lock_obj->next;
				}
				*/



				// Case : This is the first time that I make acquired S lock.

				lock_t* new_lock_obj = make_new_lock_object(bucket, key, trx_id, lock_mode);

				// Append the new lock object behind of the record lock list. (update record lock list)		
				if (pred_same_rid_lock_obj != NULL)
				{
					pred_same_rid_lock_obj->same_rid_next = new_lock_obj;
					new_lock_obj->same_rid_prev = pred_same_rid_lock_obj;
				}
				
				// Case : There's no lock object in bucket lock list
				if (bucket->head == NULL)
				{
					bucket->head = new_lock_obj;
					bucket->tail = new_lock_obj;	
				}
				// Case : There are lock objects in front of the new lock object.
				else 
				{
					bucket->tail->next = new_lock_obj;
					new_lock_obj->prev = bucket->tail;
					bucket->tail = new_lock_obj;
					new_lock_obj->next = NULL;
				}	
				new_lock_obj->lock_state = ACQUIRED;

				/* This code is for lock compression */
				//new_lock_obj->bitmap[record_index] = true;

				pthread_mutex_unlock(&lock_manager_latch);
				return PASS_WITH_PAGE;
			}
			else if (lock_mode == EXCLUSIVE)
			{
				// Do implicit locking, Don't need to make lock object.
				pthread_mutex_unlock(&lock_manager_latch);
				return PASS_WITH_PAGE;
			}
		}
		else if (result_conflict_check.first == IMPLICIT_ALIVE_NOT_CONVERTED)
		{

			/* trx manager latch ON */
			
			// Convert implicit => explicit
			lock_t* new_lock_obj = make_new_lock_object(bucket, key, result_conflict_check.second, EXCLUSIVE);	

			// Case : There's no lock object in bucket lock list
			if (bucket->head == NULL)
			{
				bucket->head = new_lock_obj;
				bucket->tail = new_lock_obj;	
			}
			// Case : There are lock objects in front of the new lock object.
			else 
			{
				bucket->tail->next = new_lock_obj;
				new_lock_obj->prev = bucket->tail;
				bucket->tail = new_lock_obj;
				new_lock_obj->next = NULL;
			}	
			new_lock_obj->lock_state = ACQUIRED;
		
			// Append the new lock object behind of the record lock list. (update record lock list)		
			if (pred_same_rid_lock_obj != NULL)
			{
				pred_same_rid_lock_obj->same_rid_next = new_lock_obj;
				new_lock_obj->same_rid_prev = pred_same_rid_lock_obj;
			}
	
			buf_page_latch_release(table_id, page_id);

			
			lock_t* new_succ_lock_obj = make_new_lock_object(bucket, key, trx_id, lock_mode);
	
			// Append the new_succ_lock_obj behind of the converted explicit lock object.
			new_lock_obj->same_rid_next = new_succ_lock_obj;
			new_succ_lock_obj->same_rid_prev = new_lock_obj;

			// Append the new succ lock object behind of the bucket lock list. (update bucket lock list)
			bucket->tail->next = new_succ_lock_obj;
			new_succ_lock_obj->prev = bucket->tail;
			bucket->tail = new_succ_lock_obj;
			new_succ_lock_obj->next = NULL;


			/* This code is for lock compression */
			//if (lock_mode == SHARED) new_succ_lock_obj->bitmap[record_index] = true;


			// new_succ_lock is conflict with new_lock_obj.
			new_succ_lock_obj->lock_state = WAITING;
			trx_table[trx_id].trx_state = SLEEPING;
						
			if (check_deadlock(new_succ_lock_obj, record_index) == true)
			{
				trx_abort(trx_id);
				pthread_mutex_unlock(&trx_manager_latch);
				pthread_mutex_unlock(&lock_manager_latch);

				return IS_DEADLOCK;
			}
	

			pthread_mutex_unlock(&trx_manager_latch);
			pthread_cond_wait(&(new_succ_lock_obj->cond), &lock_manager_latch);

			new_succ_lock_obj->lock_state = ACQUIRED;
			trx_table[trx_id].trx_state = WORKING;

			pthread_mutex_unlock(&lock_manager_latch);

			return PASS_WITHOUT_PAGE;
		}
		else if (result_conflict_check.first == IS_CONFLICT_LOCK || result_conflict_check.first == IMPLICIT_ALIVE_CONVERTED || result_conflict_check.first == CONFLICT_IN_COMPRESSION)
		{
			buf_page_latch_release(table_id, page_id);

			lock_t* new_lock_obj = make_new_lock_object(bucket, key, trx_id, lock_mode);	

			// Case : There are lock objects in front of the new lock object.
			bucket->tail->next = new_lock_obj;
			new_lock_obj->prev = bucket->tail;
			bucket->tail = new_lock_obj;
			new_lock_obj->next = NULL;
			
			// Append the new lock object behind of the record lock list. (update record lock list)		
			if (pred_same_rid_lock_obj != NULL)
			{
				pred_same_rid_lock_obj->same_rid_next = new_lock_obj;
				new_lock_obj->same_rid_prev = pred_same_rid_lock_obj;
			}

			/* This code is for lock compression */
			//if (lock_mode == SHARED) new_lock_obj->bitmap[record_index] = true;


			// Order is important. it must be before check_deadlock().
			new_lock_obj->lock_state = WAITING;
			trx_table[trx_id].trx_state = SLEEPING;
				
			pthread_mutex_lock(&trx_manager_latch);
			if (check_deadlock(new_lock_obj, record_index) == true)
			{
				trx_abort(trx_id);
				pthread_mutex_unlock(&trx_manager_latch);
				pthread_mutex_unlock(&lock_manager_latch);

				return IS_DEADLOCK;
			}

			
			pthread_mutex_unlock(&trx_manager_latch);
			pthread_cond_wait(&(new_lock_obj->cond), &lock_manager_latch);

			new_lock_obj->lock_state = ACQUIRED;
			trx_table[trx_id].trx_state = WORKING;

			pthread_mutex_unlock(&lock_manager_latch);
			return PASS_WITHOUT_PAGE;
		}
	}
}

bool is_conflict_2(lock_t* pred_lock_obj, int trx_id, int64_t record_id, int lock_mode)
{
	if (pred_lock_obj == NULL) return false;
	if (pred_lock_obj->owner_trx_id == trx_id) return false;
	if (pred_lock_obj->record_id != record_id) return false;
	if (pred_lock_obj->lock_mode == SHARED && lock_mode == SHARED) return false;

	return true;
}

bool is_conflict_1(lock_t* pred_lock_obj, lock_t* succ_lock_obj)
{
	if (pred_lock_obj == NULL || succ_lock_obj == NULL) return false;
	if (pred_lock_obj->owner_trx_id == succ_lock_obj->owner_trx_id) return false;
	if (pred_lock_obj->record_id != succ_lock_obj->record_id) return false;
	if (pred_lock_obj->lock_mode == SHARED && succ_lock_obj->lock_mode == SHARED) return false;

	return true;
}


pair<int, int> is_conflict_lock_in_record_lock_list(lock_t* pred_same_rid_lock_obj, int64_t table_id, pagenum_t page_id, int trx_id, int64_t record_id, int lock_mode, int record_index, page_t* page)
{

	char delivery[PAGE_SIZE];
	Slot slot;

	memcpy(delivery, page, PAGE_SIZE);
	//buf_read_page(table_id, page_id, (page_t*)delivery);
	pthread_mutex_lock(&trx_manager_latch);
	
	memcpy(&slot, delivery+(SLOT_STARTING_POINT + SLOT_SIZE*record_index), SLOT_SIZE);
	int record_trx_id = slot.trx_id;
	
	// Case : Another trx's implicit lock is active
	if (trx_table.find(record_trx_id) != trx_table.end())
	{
		while (pred_same_rid_lock_obj != NULL)
		{
			if (pred_same_rid_lock_obj->owner_trx_id == record_trx_id && pred_same_rid_lock_obj->lock_mode == EXCLUSIVE && pred_same_rid_lock_obj->lock_state == ACQUIRED) 
			{
				//buf_page_latch_release(table_id, page_id);
				pthread_mutex_unlock(&trx_manager_latch);
				return make_pair(IMPLICIT_ALIVE_CONVERTED, -1);
			}
			pred_same_rid_lock_obj = pred_same_rid_lock_obj->same_rid_prev;
		}


		return make_pair(IMPLICIT_ALIVE_NOT_CONVERTED, record_trx_id);
	}
	

	// Case : There is conflict lock in record lock list
	while (pred_same_rid_lock_obj != NULL)
	{
		if (is_conflict_2(pred_same_rid_lock_obj, trx_id, record_id, lock_mode)) 
		{
			//buf_page_latch_release(table_id, page_id);
			pthread_mutex_unlock(&trx_manager_latch);
			return make_pair(IS_CONFLICT_LOCK, -1);
		}
		pred_same_rid_lock_obj = pred_same_rid_lock_obj->same_rid_prev;
	}
	

	/* This code is for lock compression */
	/* 
	// Case : There is conflict lock in lock compression.
	if (lock_mode == EXCLUSIVE)
	{
		lock_Bucket* bucket = search_lock_table(table_id, page_id);	
		lock_t* temp_lock_obj = bucket->head;
		
		while (temp_lock_obj != NULL)
		{
			if (temp_lock_obj->lock_mode == SHARED && temp_lock_obj->lock_state == ACQUIRED)
			{
				if (temp_lock_obj->bitmap[record_index] == true) 
				{
					//cout << "conflict in compression \n";

					buf_page_latch_release(table_id, page_id);
					pthread_mutex_unlock(&trx_manager_latch);
					return make_pair(CONFLICT_IN_COMPRESSION, -1);
				}
			}
			temp_lock_obj = temp_lock_obj->next;
		}
	}
	*/

	// There is not conflict lock
	//buf_page_latch_release(table_id, page_id);
	pthread_mutex_unlock(&trx_manager_latch);
	return make_pair(NO_CONFLICT, -1);
}


// record_index is for lock compression
bool check_deadlock(lock_t* new_lock_obj, int record_index)
{
	// record A : S(T1) - X(T2) - X(T3)  <== X(T4)!! DEADLOCK
	// record B : S(T4) - X(T3)


	std::map<int, trx_t>::iterator iter;
	for (iter = trx_table.begin(); iter != trx_table.end(); iter++)
		iter->second.visited = false;
	
	lock_t* pred_same_rid_lock_obj = new_lock_obj->same_rid_prev;
	while (pred_same_rid_lock_obj != NULL)
	{
		if (is_conflict_1(pred_same_rid_lock_obj, new_lock_obj) && trx_table[pred_same_rid_lock_obj->owner_trx_id].trx_state == SLEEPING)
		{
			if (is_cycle(pred_same_rid_lock_obj->owner_trx_id, new_lock_obj->owner_trx_id)) 
			{
				return true; 
			}
		}
		pred_same_rid_lock_obj = pred_same_rid_lock_obj->same_rid_prev;
	}

	/*
	// for lock compression
	if (new_lock_obj->lock_mode == EXCLUSIVE)
	{
		lock_t* temp_lock_obj = new_lock_obj->p_sentinel->head;
		
		// Search bucket lock list
		while (temp_lock_obj != NULL)
		{
			if (temp_lock_obj->lock_mode == SHARED && temp_lock_obj->lock_state == ACQUIRED)
			{
				if (temp_lock_obj->bitmap[record_index] == true && trx_table[temp_lock_obj->owner_trx_id].trx_state == SLEEPING) 
				{
					if (is_cycle(temp_lock_obj->owner_trx_id, new_lock_obj->owner_trx_id)) 
					{
						//cout << "new find cycle \n";
						return true; 
					}
				}
			}
			temp_lock_obj = temp_lock_obj->next;
		}
	}
	*/

	return false;
}	

bool is_cycle(int suspicious_trx_id, int new_trx_id)
{
	// check trx_lock_list in the trx_obj which trx_id is suspicious_trx_id 
	// if there is a lock that waiting trx which trx_id is new_trx_id, then it means there is a cycle.
	// Use BFS.	
	
	if (trx_table[suspicious_trx_id].visited) return false;
	
	trx_table[suspicious_trx_id].visited = true;
	
	bool is_found = false;
	std::queue<int> q;
	q.push(suspicious_trx_id);
	while (!q.empty())
	{
		int qSize = q.size();
		for (int i = 0; i < qSize; i++)
		{
			int curr_trx_id = q.front();
			q.pop();
			trx_t& curr_trx_obj = trx_table[curr_trx_id];
			
			// Iterate trx_lock_list. (downward)
			lock_t* trx_lock_obj = curr_trx_obj.head_trx_lock_list;
			while (trx_lock_obj != NULL)
			{
				if (trx_lock_obj->lock_state == WAITING)
				{
					lock_t* prev_same_rid_lock_obj = trx_lock_obj->same_rid_prev;
					
					// Iterate record lock list (leftward)
					while (prev_same_rid_lock_obj != NULL)
					{
						// check waits for what
						if (is_conflict_1(prev_same_rid_lock_obj, trx_lock_obj) && trx_table[prev_same_rid_lock_obj->owner_trx_id].trx_state == SLEEPING)
						{
							// This means that there is cycle.
							if (prev_same_rid_lock_obj->owner_trx_id == new_trx_id) return true;
							
							trx_t& temp_trx_obj = trx_table[prev_same_rid_lock_obj->owner_trx_id];
							if (temp_trx_obj.visited != true)
							{
								q.push(prev_same_rid_lock_obj->owner_trx_id);
								temp_trx_obj.visited = true;
							}
						}
						prev_same_rid_lock_obj = prev_same_rid_lock_obj->same_rid_prev;
					}

				}
				/*
				// for lock compression
				if (trx_lock_obj->lock_state == WAITING && trx_lock_obj->lock_mode == EXCLUSIVE)
				{
					lock_t* temp_lock_obj = trx_lock_obj->p_sentinel->head;
					
					Leaf_page* leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
					char delivery[PAGE_SIZE] = {};
					Slot slot;

					int64_t table_id = trx_lock_obj->p_sentinel->table_id;
					pagenum_t page_id = trx_lock_obj->p_sentinel->page_id;

					buf_read_page(table_id, page_id, (page_t*)leaf_page);
					buf_page_latch_release(table_id, page_id);
							
					memcpy(delivery, leaf_page, PAGE_SIZE);

					long low = 0, high = leaf_page->numOfkeys-1, mid = 0;
					while (low <= high)
    				{
     				   	mid = (low + high) / 2;

						memcpy(&slot, delivery+(SLOT_STARTING_POINT+SLOT_SIZE*mid), SLOT_SIZE);
						//cout << slot.key;
       				 	if ( slot.key == trx_lock_obj->record_id ) break;
     				   	else if ( slot.key > trx_lock_obj->record_id ) high = mid-1;
      				  	else low = mid+1;
    				}
							
					int record_index = mid;
					free(leaf_page);	
					
					// Search bucket lock list
					while (temp_lock_obj != NULL)
					{
						if (temp_lock_obj->lock_mode == SHARED && temp_lock_obj->lock_state == ACQUIRED)
						{	
							if (temp_lock_obj->bitmap[record_index] == true && trx_table[temp_lock_obj->owner_trx_id].trx_state == SLEEPING) 
							{
							
								if (temp_lock_obj->owner_trx_id == new_trx_id) 
								{
									//cout << "find deadlock in compression \n";
									return true;
								}
								trx_t& temp_trx_obj = trx_table[temp_lock_obj->owner_trx_id];
								if (temp_trx_obj.visited != true)
								{
									//cout << "compression not visited\n";
									q.push(temp_lock_obj->owner_trx_id);
									temp_trx_obj.visited = true;
								}
							}
						}
						temp_lock_obj = temp_lock_obj->next;
					}	
					
				}
				*/
				trx_lock_obj = trx_lock_obj->trx_next_lock;
			}
		}
	}

	return false;
}

int lock_release(lock_t* lock_obj)  
{
	lock_Bucket* bucket = lock_obj->p_sentinel;
	
	// Case : There's only one lock object in lock list in bucket, then free the bucket.
	if (bucket->head->next == NULL)
	{
		bucket->head = NULL;
		bucket->tail = NULL;
		free(lock_obj);
		return 0;
	}
	/* This code is for lock compression
	else if (lock_obj->lock_mode == SHARED && lock_obj->lock_state == ACQUIRED)
	{
		char delivery[PAGE_SIZE];
		Slot slot;

		int64_t table_id = lock_obj->p_sentinel->table_id;
		pagenum_t page_id = lock_obj->p_sentinel->page_id;

		buf_read_page(table_id, page_id, (page_t*)delivery);
		buf_page_latch_release(table_id, page_id);

		for (int i = 0; i < MAX_NUM_OF_SLOT; i++)
		{
			if (lock_obj->bitmap[i] == true)
			{
				memcpy(&slot, delivery+(SLOT_STARTING_POINT + SLOT_SIZE * i), SLOT_SIZE);
				int record_id = slot.key;

				lock_t* temp_lock_obj = lock_obj->p_sentinel->head;

				// First, find record lock list
				while (temp_lock_obj != NULL)
				{
					if (temp_lock_obj->record_id == record_id) break;
					temp_lock_obj = temp_lock_obj->next;

				}
				if (temp_lock_obj == NULL) return false;


				// Released lock is compressed S lock.
				// If another lock is acquried, then nothing to do.
				// Else no one else is not acquired, then awake.
				if (!is_acquired_lock_in_record_lock_list(lock_obj))
				{
					lock_t* next_same_rid_lock_obj = temp_lock_obj->same_rid_next;
			
					while (next_same_rid_lock_obj != NULL)
					{
						pthread_cond_signal(&next_same_rid_lock_obj->cond);

						if (!is_conflict_1(next_same_rid_lock_obj, next_same_rid_lock_obj->same_rid_next))
							next_same_rid_lock_obj = next_same_rid_lock_obj->same_rid_next;
						else break;
					}
				}
			}
		}
	}
	*/
	else
	{
		if (!is_acquired_lock_in_record_lock_list(lock_obj))
		{
			lock_t* next_same_rid_lock_obj = lock_obj->same_rid_next;
			
			while (next_same_rid_lock_obj != NULL)
			{
				pthread_cond_signal(&next_same_rid_lock_obj->cond);

				if (!is_conflict_1(next_same_rid_lock_obj, next_same_rid_lock_obj->same_rid_next))
					next_same_rid_lock_obj = next_same_rid_lock_obj->same_rid_next;
				else break;
			}
		}
	}

	update_bucket_lock_list_after_release(bucket, lock_obj);
	update_record_lock_list_after_release(lock_obj);
	free(lock_obj);

	return 0;
}

bool is_acquired_lock_in_record_lock_list(lock_t* lock_obj)
{
	lock_t* prev_same_rid_lock_obj = lock_obj->same_rid_prev;
	lock_t* next_same_rid_lock_obj = lock_obj->same_rid_next;

	while (prev_same_rid_lock_obj != NULL)
	{
		if (prev_same_rid_lock_obj->lock_state == ACQUIRED) return true;
		prev_same_rid_lock_obj = prev_same_rid_lock_obj->same_rid_prev;
	}

	while (next_same_rid_lock_obj != NULL)
	{
		if (next_same_rid_lock_obj->lock_state == ACQUIRED) return true;
		next_same_rid_lock_obj = next_same_rid_lock_obj->same_rid_next;
	}

	return false;
}

void update_bucket_lock_list_after_release(lock_Bucket* bucket, lock_t* lock_obj)
{
	if (lock_obj == bucket->head)
	{
		bucket->head = lock_obj->next;
		lock_obj->next->prev = NULL;
	}
	else if (lock_obj == bucket->tail)
	{
		bucket->tail = lock_obj->prev;
		lock_obj->prev->next = NULL;
	}
	else // the lock is in intermediate position.
	{
		lock_obj->prev->next = lock_obj->next;
		lock_obj->next->prev = lock_obj->prev;
	}
}

void update_record_lock_list_after_release(lock_t* lock_obj)
{
	if (lock_obj->same_rid_prev == NULL && lock_obj->same_rid_next == NULL) return;

	if (lock_obj->same_rid_prev == NULL)
	{
		lock_obj->same_rid_next->same_rid_prev = NULL;
	}
	else if (lock_obj->same_rid_next == NULL)
	{
		lock_obj->same_rid_prev->same_rid_next = NULL;
	}
	else
	{
		lock_obj->same_rid_prev->same_rid_next = lock_obj->same_rid_next;
		lock_obj->same_rid_next->same_rid_prev = lock_obj->same_rid_prev;
	}
}

int trx_begin()
{
	// TRX_ID should be unique for each trx, so lock trx_table.
	pthread_mutex_lock(&trx_manager_latch);	
	
	trx_t trx_obj;
	trx_obj.trx_id = TRX_ID++; // unique transaction id (>=1)
	trx_obj.head_trx_lock_list = NULL;
	trx_obj.visited = false;
	trx_obj.trx_state = WORKING;
	
	trx_obj.last_LSN = curr_LSN;

	trx_table[trx_obj.trx_id] = trx_obj;

	pthread_mutex_unlock(&trx_manager_latch);

	pthread_mutex_lock(&log_manager_latch);
	make_LogRecord_1(-1, trx_obj.trx_id, BEGIN);
	pthread_mutex_unlock(&log_manager_latch);

	if (trx_obj.trx_id >= 1) return trx_obj.trx_id;
	else return 0;
}

int trx_commit(int trx_id)
{
	pthread_mutex_lock(&trx_manager_latch);
	
	lock_t* head = trx_table[trx_id].head_trx_lock_list;
	int64_t last_LSN = trx_table[trx_id].last_LSN;
	trx_table.erase(trx_id);
	pthread_mutex_unlock(&trx_manager_latch);
	
	lock_t* mov_temp = head;
	lock_t* del_temp = NULL;


	pthread_mutex_lock(&lock_manager_latch);
	while (mov_temp != NULL)
	{
		del_temp = mov_temp;
		mov_temp = del_temp->trx_next_lock;

		// pthread_mutex_lock(&lock_manager_latch);
		if (lock_release(del_temp) != 0) return 0;
		// pthread_mutex_unlock(&lock_manager_latch);
	}
	pthread_mutex_unlock(&lock_manager_latch);

	pthread_mutex_lock(&log_manager_latch);
	make_LogRecord_1(last_LSN, trx_id, COMMIT);
	log_buf_force();
	pthread_mutex_unlock(&log_manager_latch);

	return trx_id;
}

void trx_write_log(int64_t table_id, pagenum_t page_id, int64_t key, char* old_val, uint64_t old_val_size, int trx_id)
{
	old_log_t* old_log_obj = (old_log_t*)malloc(sizeof(old_log_t));

	old_log_obj->table_id = table_id;
	old_log_obj->page_id = page_id;
	old_log_obj->key = key;
	memcpy(old_log_obj->old_val, old_val, old_val_size);
	old_log_obj->old_val_size = old_val_size;

	trx_table[trx_id].old_logs.push(old_log_obj);
}

int trx_roll_back_record(std::stack<old_log_t*> old_logs)
{
	Leaf_page* leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
	Slot slot;
	char delivery[PAGE_SIZE] = {};
	while (!old_logs.empty())
	{
		old_log_t* old_log = old_logs.top();
		old_logs.pop();

		int64_t table_id = old_log->table_id;
		pagenum_t page_id = old_log->page_id;
		int64_t key = old_log->key;


		buf_read_page(table_id, page_id, (page_t*)leaf_page); // page locked.			

		memcpy(delivery, leaf_page, PAGE_SIZE);
		
		long low = 0, high = leaf_page->numOfkeys-1, mid = 0;
		while (low <= high)
    	{
        	mid = (low + high) / 2;
			memcpy(&slot, delivery+(SLOT_STARTING_POINT+SLOT_SIZE*mid), SLOT_SIZE);
        	if ( slot.key == key ) break;
        	else if ( slot.key > key ) high = mid-1;
        	else low = mid+1;
    	}
		if (low > high)
		{
			buf_page_latch_release(table_id, page_id);
			return -1;
		}
			
		memcpy(delivery+slot.offset, old_log->old_val, old_log->old_val_size);
		buf_write_page(table_id, page_id, (page_t*)delivery); // page unlock
	
		free(old_log);
	}
	free(leaf_page);
	
	return 0;
}


int trx_abort(int trx_id)
{
	trx_t& trx_obj = trx_table[trx_id];
	
	// 1. Undo all modified records by the transaction
	//trx_roll_back_record(trx_obj.old_logs);

	pthread_mutex_lock(&log_manager_latch);
	roll_back(trx_id);
	pthread_mutex_unlock(&log_manager_latch);


	lock_t* head = trx_obj.head_trx_lock_list;

	
	// 2. Release all acquired lock object
	lock_t* mov_temp = head;
	lock_t* del_temp = NULL;

	while (mov_temp != NULL)
	{
		del_temp = mov_temp;
		mov_temp = del_temp->trx_next_lock;
		if (lock_release(del_temp) != 0) return 0;
	}

	// 3. Remove the transaction table entry \n";
	trx_table.erase(trx_id);

	



	return trx_id;
}

