#include "disk_based_bpt.h"
#include <iostream>
#include <cstring>

using namespace std;

int TABLE_CNT = 0;
Header_page* header_page;

int64_t open_table(char* pathname)
{
	int64_t table_id = file_open_table_file(pathname);
	if ( table_id >= 0 ) 
	{
		TABLE_CNT++;
		return table_id;
	}
	else return -1;
}

int db_insert (int64_t table_id, int64_t key, char* value, uint16_t val_size)
{
	char ret_val[MAX_SIZE_OF_VALUE];
	uint16_t size;
	
	// can't duplicate 
	if (db_find(table_id, key, ret_val, &size) == 0) 
		return -1;
	
	// No such key in page.
	buf_read_page(table_id, 0, (page_t*)header_page);
	pagenum_t root_pagenum = header_page->root_pagenum;
	buf_pin_out(table_id, 0);
	
	// Case : the tree does not exist yet.
	if (root_pagenum == 0 )
		return db_start_new_tree(table_id, key, value, val_size);

	// record가 들어갈 수 있는 leaf_pagenum.
	pagenum_t leaf_pagenum = db_find_leaf(table_id, key);

	Leaf_page* leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
	if ( leaf_page == NULL ) return -1;
	buf_read_page(table_id, leaf_pagenum, (page_t*)leaf_page);
	
	// Case : Leaf has room for record.
	if ( leaf_page->free_space_amount >= SLOT_SIZE + val_size )
	{
		buf_pin_out(table_id, leaf_pagenum);
		return db_insert_into_leaf(table_id, leaf_page, leaf_pagenum, key, value, val_size);
	}

	// Case : Leaf must be split.
	buf_pin_out(table_id, leaf_pagenum);
	return db_insert_into_leaf_after_splitting(table_id, leaf_page, leaf_pagenum, key, value, val_size);
}	

int db_find (int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size)
{
	int i = 0;
	Slot slot;
	char delivery[PAGE_SIZE] = {};
	char slotted_value[MAX_SIZE_OF_VALUE];

	pagenum_t leaf_pagenum = db_find_leaf(table_id, key); // record가 들어있을만한 leaf
	if ( leaf_pagenum == -1 ) return -1;

	// leaf page disk에서 읽어오기.
	Leaf_page* leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
	if ( leaf_page == NULL ) return -1;
	buf_read_page(table_id, leaf_pagenum, (page_t*)leaf_page);

	// slot 연산을 위해서 delivery에 복사.
	memcpy(delivery, leaf_page, PAGE_SIZE);
	
	long low = 0, high = leaf_page->numOfkeys-1, mid = 0;
	while( low <= high )
    {
        mid = (low + high) / 2;
		memcpy( &slot, delivery+(SLOT_STARTING_POINT+SLOT_SIZE*mid), SLOT_SIZE);
        if ( slot.key == key ) break;
        else if ( slot.key > key ) high = mid-1;
        else low = mid+1;
    }
    if ( low > high ) return -1;


	memcpy(ret_val, delivery+slot.offset, slot.size);
	memcpy(val_size, delivery+(SLOT_STARTING_POINT+SLOT_SIZE*mid)+sizeof(uint64_t), sizeof(uint16_t));

	buf_pin_out(table_id, leaf_pagenum);
	free(leaf_page);
	return 0;
}

int db_delete (int64_t table_id, int64_t key)
{
	char ret_val[MAX_SIZE_OF_VALUE];
  	uint16_t size;

	int flag = db_find(table_id, key, ret_val, &size);
	
	if ( flag == 0 )  
	{
		int key_leaf_pagenum = db_find_leaf(table_id, key);
		return db_delete_entry(table_id, key_leaf_pagenum, key);
	}
	return -1;
}

int init_db(int num_buf)
{
	header_page = (Header_page*)malloc(sizeof(Header_page));
	if ( header_page == NULL ) return -1;
	if ( TABLE_CNT >= 20 ) return -1;
	return buf_init_db(num_buf);
}

int shutdown_db()
{
	free(header_page);
	int ret = buf_shutdown_db();
	file_close_table_files();
	return ret;
}

pagenum_t db_find_leaf (int64_t table_id, int64_t key)
{
	buf_read_page(table_id, 0, (page_t*)header_page);
	
	pagenum_t root_pagenum = header_page->root_pagenum;
	buf_pin_out(table_id, 0);

	if ( root_pagenum == 0 ) return -1; // 트리가 빈 경우

	Internal_page* internal_page = (Internal_page*)malloc(sizeof(Internal_page));
	buf_read_page(table_id, root_pagenum, (page_t*)internal_page);
	
	pagenum_t pagenum = root_pagenum, temp_pagenum;
	
	while ( internal_page->is_leaf != 1 )
	{
		// upper bound search
        long low = 0, high = internal_page->numOfkeys, mid = 0;
        while( low < high )
        {
            mid = ( low + high ) / 2;
            if ( key >= internal_page->entries[mid].key ) low = mid+1;
            else high = mid;
        }
        if ( low == 0 ) temp_pagenum = internal_page->leftmost_pagenum;
        else temp_pagenum = internal_page->entries[low-1].pagenum;

		buf_pin_out(table_id, pagenum);
		pagenum = temp_pagenum;
		buf_read_page(table_id, pagenum, (page_t*)internal_page);
	}

	buf_pin_out(table_id, pagenum);
	free(internal_page);

	return pagenum;
}

int db_start_new_tree(int64_t table_id, int64_t key, char* value, uint64_t val_size)
{
	char delivery[PAGE_SIZE] = {};
	pagenum_t pagenum = buf_alloc_page(table_id);
	

	Leaf_page* leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
	if ( leaf_page == NULL ) return -1;

	buf_read_page(table_id, pagenum, (page_t*)leaf_page);
	buf_read_page(table_id, 0, (page_t*)header_page);
	
	header_page->root_pagenum = pagenum;

	Slot slot;
  	slot.key = key;
   	slot.size = val_size;
  	slot.offset = PAGE_SIZE - val_size;

	leaf_page->parent_pagenum = 0;
	leaf_page->is_leaf = 1;
	leaf_page->numOfkeys = 1;
	leaf_page->free_space_amount = INITIAL_FREE_SPACE_AMOUNT - (SLOT_SIZE + val_size);
	leaf_page->sibling_pagenum = 0;
	
	memcpy(delivery, leaf_page, PAGE_SIZE);
	memcpy(delivery+SLOT_STARTING_POINT, &slot, SLOT_SIZE);
	memcpy(delivery+slot.offset, value, val_size);
	memcpy(leaf_page, delivery, PAGE_SIZE);
	
	buf_write_page(table_id, pagenum, (page_t*)leaf_page);
	buf_write_page(table_id, 0, (page_t*)header_page);
	
	free(leaf_page);
	return 0;
}

int db_insert_into_leaf(int64_t table_id, Leaf_page* p_leaf_page, pagenum_t leaf_pagenum, int64_t key, char* value, uint16_t val_size)
{
	int i, k, insertion_point;
	Slot slot;
	char delivery[PAGE_SIZE] = {};

	Leaf_page* leaf_page = p_leaf_page;

	// copy page to delivery to use offset by 1.
	memcpy(delivery, leaf_page, PAGE_SIZE);
	
	// upper_bound search ( binary search )
    long low = 0, high = leaf_page->numOfkeys, mid = 0;
    while( low < high )
    {
        mid = ( low + high ) / 2;
        memcpy(&slot, delivery+(SLOT_STARTING_POINT+SLOT_SIZE*mid), SLOT_SIZE);

        if ( key >= slot.key ) low = mid+1;
        else high = mid;
    }
    insertion_point = low;

    // move slots like dummy.
    if ( insertion_point != leaf_page->numOfkeys )
    {
        uint32_t num = leaf_page->numOfkeys - insertion_point;
        memmove(delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(insertion_point+1)), delivery+(SLOT_STARTING_POINT+SLOT_SIZE*insertion_point), SLOT_SIZE*num);
    }


	// slot initialize
	slot.key = key;
	slot.size = val_size;
	slot.offset = (SLOT_STARTING_POINT+SLOT_SIZE*leaf_page->numOfkeys+ leaf_page->free_space_amount) - val_size;


	// insert slot in delivery
	memcpy(delivery+(SLOT_STARTING_POINT+SLOT_SIZE*insertion_point), &slot, SLOT_SIZE);
	memcpy(delivery+slot.offset, value, val_size);

	// copy delivery to in-memory leaf_page.
	memcpy(leaf_page, delivery, PAGE_SIZE);
	
	// update page header
	leaf_page->numOfkeys++;
	leaf_page->free_space_amount -= (SLOT_SIZE+val_size);

	// write leaf_page to frame.
	buf_write_page(table_id, leaf_pagenum, (page_t*)leaf_page);

	free(leaf_page);
	return 0;
}

int db_insert_into_leaf_after_splitting(int64_t table_id, Leaf_page* p_old_leaf_page, pagenum_t old_leaf_pagenum, int64_t key, char* value, uint16_t val_size)
{	
	Leaf_page* old_leaf_page = p_old_leaf_page;

	Leaf_page* new_leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
	if ( new_leaf_page == NULL ) return -1;

	pagenum_t new_leaf_pagenum = buf_alloc_page(table_id);
	buf_read_page(table_id, new_leaf_pagenum, (page_t*)new_leaf_page);


	int sum = 0, insertion_point = 0, i = 0, j = 0, k = 0;
	bool done = false;
	Slot slot, new_slot;
	
	char temp_page[PAGE_SIZE] = {};
	char old_delivery[PAGE_SIZE] = {};
	char new_delivery[PAGE_SIZE] = {};
	char slotted_value[MAX_SIZE_OF_VALUE];
	char cleaner[MAX_SIZE_OF_VALUE] = {};

	pagenum_t old_parent_pagenum = old_leaf_page->parent_pagenum;
	pagenum_t old_sibling_pagenum = old_leaf_page->sibling_pagenum;
	uint64_t old_amount = old_leaf_page->free_space_amount;
	uint64_t new_amount = INITIAL_FREE_SPACE_AMOUNT;
	uint32_t old_keys = old_leaf_page->numOfkeys;
	uint32_t new_keys = 0;
	uint16_t temp_offset = PAGE_SIZE;
	

	memcpy(old_delivery, old_leaf_page, PAGE_SIZE);

	int key_insert_point = 0;
	uint16_t before_offset = SLOT_STARTING_POINT + SLOT_SIZE * old_keys;
	// find split point
	for( i = 0; i < old_leaf_page->numOfkeys; i++ )
	{
		memcpy(&slot, old_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*i), SLOT_SIZE);
		sum += (slot.size + SLOT_SIZE);
		
		if ( key > slot.key ) key_insert_point = i+1;
		if ( sum >= SPLIT_THRESHOLD ) break;
	}


	for ( j = i ; j < old_leaf_page->numOfkeys; j++ )
	{
		// get slot to be moved.
		memcpy(&slot, old_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*j), SLOT_SIZE);
		memcpy(slotted_value, old_delivery+(slot.offset), slot.size);

		// clean the new free space.	
        memcpy(old_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*j), cleaner, SLOT_SIZE);
        memcpy(old_delivery+(slot.offset), cleaner, slot.size);

		temp_offset -= slot.size;

		// insert되는 키가 new_leaf_page에 써지는 경우. & key값이 기존에있던 key 값보다 작았던 경우
		if ( key_insert_point >= i && key < slot.key && !done)
		{
			memcpy(old_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*j), &slot, SLOT_SIZE);
			memcpy(old_delivery+(slot.offset), slotted_value, slot.size);

			temp_offset += (slot.size - val_size);

			slot.key = key;
			slot.size = val_size;

			strcpy(slotted_value, value);

			done = true;
			j--;

			slot.offset = temp_offset;
			memcpy(new_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*insertion_point), &slot, SLOT_SIZE);
        	memcpy(new_delivery+temp_offset, slotted_value, slot.size);
			insertion_point++;
			new_amount -= (SLOT_SIZE+slot.size);
			new_keys++;
			
			continue;
		}
		
		slot.offset = temp_offset;

		memcpy(new_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*insertion_point), &slot, SLOT_SIZE);
        memcpy(new_delivery+temp_offset, slotted_value, slot.size);

		insertion_point++;

		old_amount += (SLOT_SIZE+slot.size);
		new_amount -= (SLOT_SIZE+slot.size);
		old_keys--;
		new_keys++;	
	}

	// 입력된 key값이 가장 커서 들어가지 않았던 경우
	if ( key_insert_point >= i && !done )
	{
		temp_offset -= val_size;

        slot.key = key; 
        slot.size = val_size;
		slot.offset = temp_offset;

        strcpy(slotted_value, value);

		memcpy(new_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*insertion_point), &slot, SLOT_SIZE);
        memcpy(new_delivery+temp_offset, slotted_value, slot.size);
		
		new_amount -= (SLOT_SIZE+slot.size);
        new_keys++;
		done = true;
	}
	
	// insert되는 키가 old_leaf_page에 써지는 경우.
	if ( key_insert_point < i )
	{
		for ( k = old_keys; k > key_insert_point; k-- )
			memcpy(old_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*k), old_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(k-1)), SLOT_SIZE);

		slot.key = key;
		slot.size = val_size;
		slot.offset = before_offset - val_size;

		memcpy(old_delivery+(SLOT_STARTING_POINT + SLOT_SIZE*key_insert_point), &slot , SLOT_SIZE);
		memcpy(old_delivery+slot.offset, value, val_size);

		old_amount -= (SLOT_SIZE + val_size);
		old_keys++;
	}


	// values in old_leaf_page should be compacted.
	memcpy(temp_page, old_leaf_page, PAGE_SIZE);
	memcpy(temp_page+SLOT_STARTING_POINT, cleaner, PAGE_SIZE-SLOT_STARTING_POINT);
	temp_offset = PAGE_SIZE;
	for ( i = 0; i < old_keys; i++ )
	{
		memcpy(&slot, old_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*i), SLOT_SIZE);
        memcpy(slotted_value, old_delivery+slot.offset, slot.size);
	
		temp_offset -= slot.size;
		slot.offset = temp_offset;

		memcpy(temp_page+(SLOT_STARTING_POINT+SLOT_SIZE*i), &slot, SLOT_SIZE);
		memcpy(temp_page+temp_offset, slotted_value, slot.size);
	}


	// move delivery to in-memory page
	memcpy(old_leaf_page, temp_page, PAGE_SIZE);
	memcpy(new_leaf_page, new_delivery, PAGE_SIZE);


	// update page header
	new_leaf_page->parent_pagenum = old_parent_pagenum;
 	new_leaf_page->sibling_pagenum = old_sibling_pagenum;
 	new_leaf_page->is_leaf = 1;
	new_leaf_page->numOfkeys = new_keys;
	new_leaf_page->free_space_amount = new_amount;

	old_leaf_page->parent_pagenum = old_parent_pagenum;
	old_leaf_page->sibling_pagenum = new_leaf_pagenum;
	old_leaf_page->is_leaf = 1;
	old_leaf_page->numOfkeys = old_keys;
	old_leaf_page->free_space_amount = old_amount;


	// fetch the first slot.key in new_leaf_page to insert its parent.
	memcpy(&new_slot, new_delivery+SLOT_STARTING_POINT, SLOT_SIZE);
	int64_t new_key = new_slot.key;
	
	// write in-memory page to disk_file.
	buf_write_page(table_id, old_leaf_pagenum, (page_t*)old_leaf_page);
	buf_write_page(table_id, new_leaf_pagenum, (page_t*)new_leaf_page);

	free(old_leaf_page);
    free(new_leaf_page);

	return db_insert_into_parent(table_id, old_leaf_pagenum, new_key, new_leaf_pagenum, old_parent_pagenum);
}


int db_insert_into_parent(int64_t table_id, pagenum_t left_pagenum, uint64_t key, pagenum_t right_pagenum, pagenum_t parent_pagenum)
{
	// No parent == internal_page is root page.
	if ( parent_pagenum == 0 )
		return db_insert_into_new_root(table_id, left_pagenum, key, right_pagenum);

	// has parent page.
	Internal_page* parent_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( parent_page == NULL ) return -1;

	buf_read_page(table_id, parent_pagenum, (page_t*)parent_page);


	int left_index = 0; // -1 ~ 246
	if ( parent_page->leftmost_pagenum == left_pagenum ) left_index = -1;
	else 
	{
		while( left_index < parent_page->numOfkeys && parent_page->entries[left_index].pagenum != left_pagenum )
			left_index++;
	}

	// parent_page has room for key to be inserted.
	if( parent_page->numOfkeys < INTERNAL_ORDER - 1 )
	{
		// like insert_into_node()
		int i;
		for ( i = parent_page->numOfkeys; i > left_index; i-- )
		{
			parent_page->entries[i].key = parent_page->entries[i-1].key;
			parent_page->entries[i].pagenum = parent_page->entries[i-1].pagenum;
		}

		// update parent_page
		parent_page->entries[left_index+1].key = key;
		parent_page->entries[left_index+1].pagenum = right_pagenum;
		parent_page->numOfkeys++;

		buf_write_page(table_id, parent_pagenum, (page_t*)parent_page);
        free(parent_page);
		return 0;
	}
	
	buf_pin_out(table_id, parent_pagenum);
	return db_insert_into_internal_after_splitting(table_id, parent_page,  parent_pagenum, left_index, key, right_pagenum);
}

int db_insert_into_new_root(int64_t table_id, pagenum_t left_pagenum, uint64_t key, pagenum_t right_pagenum)
{
	pagenum_t root_pagenum = buf_alloc_page(table_id);

	Internal_page* root_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( root_page == NULL ) return -1;
    Internal_page* left_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( left_page == NULL ) return -1;
    Internal_page* right_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( right_page == NULL ) return -1;

	buf_read_page(table_id, 0, (page_t*)header_page);
	buf_read_page(table_id, root_pagenum, (page_t*)root_page);
	buf_read_page(table_id, left_pagenum, (page_t*)left_page);
	buf_read_page(table_id, right_pagenum, (page_t*)right_page);

	header_page->root_pagenum = root_pagenum;
	root_page->parent_pagenum = 0;
	root_page->is_leaf = 0;
	root_page->numOfkeys = 1;
	root_page->leftmost_pagenum = left_pagenum;
	root_page->entries[0].key = key;
	root_page->entries[0].pagenum = right_pagenum;
	left_page->parent_pagenum = root_pagenum;
	right_page->parent_pagenum = root_pagenum;

	buf_write_page(table_id, 0, (page_t*)header_page);
	buf_write_page(table_id, root_pagenum, (page_t*)root_page);
	buf_write_page(table_id, left_pagenum, (page_t*)left_page);
	buf_write_page(table_id, right_pagenum, (page_t*)right_page);

	free(root_page);
    free(left_page);
    free(right_page);
	return 0;
}

int db_insert_into_internal_after_splitting(int64_t table_id, Internal_page* p_old_page, pagenum_t old_pagenum, int left_index, int64_t key, pagenum_t right_pagenum)
{
	Internal_page* old_page = p_old_page;

	Internal_page* new_page = (Internal_page*)malloc(sizeof(Internal_page));
	if ( new_page == NULL ) return -1;

	Internal_page* child_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( child_page == NULL ) return -1;

	buf_read_page(table_id, old_pagenum, (page_t*)old_page);

	pagenum_t parent_pagenum = old_page->parent_pagenum;
	pagenum_t new_pagenum = buf_alloc_page(table_id);
	buf_read_page(table_id, new_pagenum, (page_t*)new_page);

	new_page->is_leaf = 0;
	new_page->numOfkeys = 0;
	new_page->leftmost_pagenum = 0;
	new_page->parent_pagenum = parent_pagenum;

	Entry* temp_entries = (Entry*)malloc(sizeof(Entry) * INTERNAL_ORDER);

	// move all old_page data to temp_entries
	int i, j;
	for ( i = 0, j = 0; i < old_page->numOfkeys; i++, j++ )
	{
		if ( j == left_index + 1 ) j++;
		temp_entries[j].key = old_page->entries[i].key;
		temp_entries[j].pagenum = old_page->entries[i].pagenum;
	}

	temp_entries[left_index+1].key = key;
	temp_entries[left_index+1].pagenum = right_pagenum;

	int split = db_cut(INTERNAL_ORDER); // 125
	old_page->numOfkeys = 0;
	
	// move left half of data to old_page
	for ( i = 0; i < split-1; i++ )
	{
		old_page->entries[i].key = temp_entries[i].key;
		old_page->entries[i].pagenum = temp_entries[i].pagenum;
		old_page->numOfkeys++;
	}

	int k_prime = temp_entries[split-1].key;
	new_page->leftmost_pagenum = temp_entries[split-1].pagenum;
	
	// move right half of data to new_page
	for ( ++i, j = 0; i < INTERNAL_ORDER; i++, j++ )
	{
		new_page->entries[j].key = temp_entries[i].key;
		new_page->entries[j].pagenum = temp_entries[i].pagenum;
		new_page->numOfkeys++;
	}	
	
	// update child_page's parent_pagenum. 
	for ( i = 0; i <= new_page->numOfkeys; i++ )
	{
		if ( i == 0 )
		{
			buf_read_page(table_id, new_page->leftmost_pagenum, (page_t*)child_page);
			child_page->parent_pagenum = new_pagenum;
			buf_write_page(table_id, new_page->leftmost_pagenum, (page_t*)child_page);
			continue;
		}
		buf_read_page(table_id, new_page->entries[i-1].pagenum, (page_t*)child_page);
		child_page->parent_pagenum = new_pagenum;
		buf_write_page(table_id, new_page->entries[i-1].pagenum, (page_t*)child_page);
	}

	buf_write_page(table_id, old_pagenum, (page_t*)old_page);
	buf_write_page(table_id, new_pagenum, (page_t*)new_page);

	free(temp_entries);
	free(old_page);
    free(new_page);
    free(child_page);
	
	return db_insert_into_parent(table_id, old_pagenum, k_prime, new_pagenum, parent_pagenum);
}

int db_cut( int length )
{
	if ( length % 2 == 0 ) return length/2;
	else return length/2 + 1;
}

int db_delete_entry(int64_t table_id, pagenum_t pagenum, uint64_t key)
{
	pagenum_t del_pagenum = db_remove_entry_from_page(table_id, pagenum, key);
	if ( del_pagenum < 0 ) return -1;

	buf_read_page(table_id, 0, (page_t*)header_page);
	pagenum_t root_pagenum = header_page->root_pagenum;
	buf_pin_out(table_id, 0);

	// Case : deletion from the root.
	if ( root_pagenum == del_pagenum )
		return db_adjust_root(table_id, del_pagenum);
	
	// Case : deletion from a node below the root.
	// Determine minimum allowable size of node to be preserved after deletion.

	Leaf_page* del_page = (Leaf_page*)malloc(sizeof(Leaf_page));
    if( del_page == NULL ) return -1;
	Leaf_page* neighbor_page = (Leaf_page*)malloc(sizeof(Leaf_page));
	if( neighbor_page == NULL ) return -1;
	Internal_page* parent_page = (Internal_page*)malloc(sizeof(Internal_page));
	if ( parent_page == NULL ) return -1;
	
	buf_read_page(table_id, del_pagenum, (page_t*)del_page);

	pagenum_t parent_pagenum = del_page->parent_pagenum;
	buf_read_page(table_id, parent_pagenum, (page_t*)parent_page);

	int min_keys = db_cut(INTERNAL_ORDER) - 1;
	int neighbor_index = get_neighbor_index(table_id, del_pagenum, parent_page);
	int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index+1;
	int k_prime = parent_page->entries[k_prime_index].key;


	pagenum_t neighbor_pagenum;
	if ( del_pagenum == parent_page->leftmost_pagenum ) neighbor_pagenum = parent_page->entries[0].pagenum;
	else if ( del_pagenum == parent_page->entries[0].pagenum ) neighbor_pagenum = parent_page->leftmost_pagenum;
	else neighbor_pagenum = parent_page->entries[neighbor_index].pagenum;

	buf_read_page(table_id, neighbor_pagenum, (page_t*)neighbor_page);

	int ret = 0;

	if ( del_page->is_leaf == 1 )
	{
		if ( del_page->free_space_amount >= THRESHOLD ) 
		{
			if ( neighbor_page->free_space_amount >= INITIAL_FREE_SPACE_AMOUNT - del_page->free_space_amount )
				ret = merge_leaf_pages(table_id, del_pagenum, neighbor_pagenum, k_prime);
			else
				ret = redistribute_leaf_pages(table_id, del_pagenum, neighbor_pagenum, k_prime_index);
		}
	}
	else
	{
		if ( del_page->numOfkeys < min_keys ) 
		{
			int capacity = INTERNAL_ORDER-1;
			if ( neighbor_page->numOfkeys + del_page->numOfkeys < capacity )
				ret = merge_internal_pages(table_id, del_pagenum, neighbor_pagenum, k_prime);
			else 
				ret = redistribute_internal_pages(table_id, del_pagenum, neighbor_pagenum, k_prime_index, k_prime, min_keys);
		}
	}
	
	buf_pin_out(table_id, del_pagenum);
	buf_pin_out(table_id, parent_pagenum);
	buf_pin_out(table_id, neighbor_pagenum);
	return ret;
}

pagenum_t db_remove_entry_from_page(int64_t table_id, pagenum_t pagenum, uint64_t key)
{
	int i, k;
	
	Leaf_page* page = (Leaf_page*)malloc(sizeof(Leaf_page));
    if( page == NULL ) return -1;

	buf_read_page(table_id, pagenum, (page_t*)page);

	// leaf page
	if ( page->is_leaf == 1 )
	{
		
		char delivery[PAGE_SIZE] = {};
    	char cleaner[MAX_SIZE_OF_VALUE] = {};
    	char temp_page[PAGE_SIZE] = {};
    	char slotted_value[MAX_SIZE_OF_VALUE];
    	uint64_t temp_offset = PAGE_SIZE;

		Slot slot;		

		Leaf_page* key_leaf_page = (Leaf_page*)page;
		memcpy(delivery, key_leaf_page, PAGE_SIZE);
	
		uint64_t key_leaf_amount = key_leaf_page->free_space_amount;
		uint32_t key_leaf_keys = key_leaf_page->numOfkeys;
		
		long  low = 0, high = key_leaf_page->numOfkeys-1, mid = 0;
        while( low <= high )
        {
            mid = (low + high) / 2;
            memcpy( &slot, delivery+(SLOT_STARTING_POINT+SLOT_SIZE*mid), SLOT_SIZE);
            if ( slot.key == key ) break;
            else if ( slot.key > key ) high = mid-1;
            else low = mid+1;
        }

	

		// clean the deleted record and slot position.
		memcpy( delivery+slot.offset, cleaner, slot.size );

		uint32_t num = key_leaf_page->numOfkeys - (mid+1);
        memmove( delivery+(SLOT_STARTING_POINT+SLOT_SIZE*mid), delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(mid+1)), SLOT_SIZE*num);

		key_leaf_amount += (SLOT_SIZE+slot.size);
		key_leaf_keys--;

		// values in leaf_page shoud be compacted.
		memcpy(temp_page, key_leaf_page, PAGE_SIZE);
		memcpy(temp_page+SLOT_STARTING_POINT, cleaner, PAGE_SIZE-SLOT_STARTING_POINT);

		for ( i = 0; i < key_leaf_page->numOfkeys-1; i++ )
		{
			memcpy(&slot, delivery+(SLOT_STARTING_POINT+SLOT_SIZE*i), SLOT_SIZE);
			memcpy(slotted_value, delivery+slot.offset, slot.size);
		
			temp_offset -= slot.size;
			slot.offset = temp_offset;

			memcpy(temp_page+(SLOT_STARTING_POINT+SLOT_SIZE*i), &slot, SLOT_SIZE);
			memcpy(temp_page+temp_offset, slotted_value, slot.size);
		}
		
		// move temp_page to in-memory page
		memcpy(key_leaf_page, temp_page, PAGE_SIZE);
		
		// update page header
		key_leaf_page->numOfkeys = key_leaf_keys;
		key_leaf_page->free_space_amount = key_leaf_amount;

		buf_write_page(table_id, pagenum, (page_t*)key_leaf_page);
	}
	else
	{
		Internal_page* internal_page = (Internal_page*)page;
		i = 0;
		// find key to be deleted.
		while (internal_page->entries[i].key != key )
			i++;
		
		// move entries one by one backward.
		for (++i; i < internal_page->numOfkeys; i++)
		{
			internal_page->entries[i-1].key = internal_page->entries[i].key;
			internal_page->entries[i-1].pagenum = internal_page->entries[i].pagenum;
		}
		
		// update page header.
		internal_page->numOfkeys--;
		buf_write_page(table_id, pagenum, (page_t*)internal_page);
	}

	free(page);
	return pagenum;
}

int db_adjust_root(int64_t table_id, pagenum_t root_pagenum)
{
	Internal_page* root_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( root_page == NULL ) return -1;
	buf_read_page(table_id, root_pagenum, (page_t*)root_page);

	// Case : nonempty root.
	if ( root_page->numOfkeys > 0 ) 
	{
		buf_pin_out(table_id, root_pagenum);
		free(root_page);
		return 0;
	}

	// Case : empty root
	// If it has a child, promote the first (only) child as the new root. -> only leftmost_pagenumm
	if ( root_page->is_leaf == 0  )
	{
		pagenum_t new_root_pagenum = root_page->leftmost_pagenum;

		// update header_page
		buf_read_page(table_id, 0, (page_t*)header_page);
		header_page->root_pagenum = new_root_pagenum;
		
		Internal_page* new_root_page = (Internal_page*)malloc(sizeof(Internal_page));
		if ( new_root_page == NULL ) return -1;

		buf_read_page(table_id, new_root_pagenum, (page_t*)new_root_page);
		new_root_page->parent_pagenum = 0;

		buf_write_page(table_id, 0, (page_t*)header_page);
		buf_write_page(table_id, new_root_pagenum, (page_t*)new_root_page);
		free(new_root_page);
	}
	else // no children and leaf page
	{
		buf_read_page(table_id, 0, (page_t*)header_page);
		header_page->root_pagenum = 0;
		buf_write_page(table_id, 0, (page_t*)header_page);
	}

	buf_free_page(table_id, root_pagenum);
	free(root_page);
	return 0;
}

int get_neighbor_index(int64_t table_id, pagenum_t del_pagenum, Internal_page* p_parent_page)
{
	Internal_page* parent_page = p_parent_page;

	int i, neighbor_index;
	for ( i = 0; i < parent_page->numOfkeys; i++ )
	{
		if ( parent_page->entries[i].pagenum == del_pagenum ) 
		{
			neighbor_index = i-1;
			break;
		}
	}
	if ( i == parent_page->numOfkeys ) neighbor_index = -1;

	return neighbor_index;
}

int merge_leaf_pages(int64_t table_id, pagenum_t leaf_pagenum, pagenum_t neighbor_pagenum, int k_prime)
{	
	Leaf_page* neighbor_page = (Leaf_page*)malloc(sizeof(Leaf_page));
    if ( neighbor_page == NULL ) return -1;
	Leaf_page* leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
    if ( leaf_page == NULL ) return -1;
	
	buf_read_page(table_id, neighbor_pagenum, (page_t*)neighbor_page);
 	buf_read_page(table_id, leaf_pagenum, (page_t*)leaf_page);

	Internal_page* parent_page = (Internal_page*)malloc(sizeof(Internal_page));
    if( parent_page == NULL ) return -1;
	pagenum_t parent_pagenum = leaf_page->parent_pagenum;
	buf_read_page(table_id, parent_pagenum, (page_t*)parent_page);


	Slot slot;
	int ret, i, j;
	char slotted_value[MAX_SIZE_OF_VALUE]; 
	char neighbor_delivery[PAGE_SIZE] = {};
	char leaf_delivery[PAGE_SIZE] = {};
	char cleaner[MAX_SIZE_OF_VALUE] = {};

	uint64_t leaf_amount = leaf_page->free_space_amount;
	uint64_t neighbor_amount = neighbor_page->free_space_amount;
	uint32_t leaf_keys = leaf_page->numOfkeys;
	uint32_t neighbor_keys = neighbor_page->numOfkeys;
	uint16_t offset;


	memcpy(neighbor_delivery, neighbor_page, PAGE_SIZE);
	memcpy(leaf_delivery, leaf_page, PAGE_SIZE);

	if (parent_page->leftmost_pagenum == leaf_pagenum) // leaf - neighbor 순서.
	{
		memcpy(leaf_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*leaf_keys), neighbor_delivery+SLOT_STARTING_POINT, SLOT_SIZE*neighbor_keys);

        offset = SLOT_STARTING_POINT + SLOT_SIZE*leaf_keys + leaf_amount;
        for ( i = 0; i < neighbor_keys; i++ )
        {
            memcpy(&slot, leaf_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(leaf_keys+i)), SLOT_SIZE);
            memcpy(slotted_value, neighbor_delivery+slot.offset, slot.size);
            offset -= slot.size;
            slot.offset = offset;
            memcpy(leaf_delivery+offset, slotted_value, slot.size);
            leaf_amount -= slot.size;
        }

        leaf_keys += neighbor_keys;
        leaf_amount -= SLOT_SIZE * neighbor_keys;
		

		// copy delivery -> in-memory page.
  		memcpy(leaf_page, leaf_delivery, PAGE_SIZE);

		// update page header
  		leaf_page->free_space_amount = leaf_amount;
  		leaf_page->numOfkeys = leaf_keys;
		leaf_page->sibling_pagenum = neighbor_page->sibling_pagenum;

		buf_write_page(table_id, leaf_pagenum, (page_t*)leaf_page);
		buf_free_page(table_id, neighbor_pagenum);
		buf_pin_out(table_id, parent_pagenum);

		ret = db_delete_entry(table_id, leaf_page->parent_pagenum, k_prime);
	}
	else // neighbor - leaf 순서
	{
		memcpy(neighbor_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*neighbor_keys), leaf_delivery+SLOT_STARTING_POINT, SLOT_SIZE*leaf_keys);

        offset = SLOT_STARTING_POINT + SLOT_SIZE*neighbor_keys + neighbor_amount;
        for( i = 0; i < leaf_keys; i++ )
        {
            memcpy(&slot, neighbor_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(neighbor_keys+i)), SLOT_SIZE);
            memcpy(slotted_value, leaf_delivery+slot.offset, slot.size);
            offset -= slot.size;
            slot.offset = offset;
            memcpy(neighbor_delivery+offset, slotted_value, slot.size);
            neighbor_amount -= slot.size;
        }
        neighbor_keys += leaf_keys;
        neighbor_amount -= SLOT_SIZE * leaf_keys;


		// copy delivery -> in-memory page
		memcpy(neighbor_page, neighbor_delivery, PAGE_SIZE);

		// update page header
		neighbor_page->free_space_amount = neighbor_amount;
  		neighbor_page->numOfkeys = neighbor_keys;
		neighbor_page->sibling_pagenum = leaf_page->sibling_pagenum;

		buf_write_page(table_id, neighbor_pagenum, (page_t*)neighbor_page);
		buf_free_page(table_id, leaf_pagenum);
		buf_pin_out(table_id, parent_pagenum);

		ret = db_delete_entry(table_id, neighbor_page->parent_pagenum, k_prime);
	}

	free(leaf_page);
    free(neighbor_page);
    free(parent_page);
	return ret;
}

int merge_internal_pages(int64_t table_id, pagenum_t internal_pagenum, pagenum_t neighbor_pagenum, int k_prime)
{
	Internal_page* internal_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( internal_page == NULL ) return -1;
	Internal_page* neighbor_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( neighbor_page == NULL ) return -1;
	Internal_page* parent_page = (Internal_page*)malloc(sizeof(Internal_page));
    if( parent_page == NULL ) return -1;
	Internal_page* child_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( child_page == NULL ) return -1;


	buf_read_page( table_id, internal_pagenum, (page_t*)internal_page);
  	buf_read_page( table_id, neighbor_pagenum, (page_t*)neighbor_page);
    buf_read_page(table_id, internal_page->parent_pagenum, (page_t*)parent_page);

	int i, j, insertion_index, end_point, ret;
	pagenum_t parent_pagenum = internal_page->parent_pagenum;

	if ( parent_page->leftmost_pagenum == internal_pagenum) // internal_page - neighbor_page 순서.
	{
		insertion_index = internal_page->numOfkeys;
		
		// Append k_prime.
		internal_page->entries[insertion_index].key = k_prime;
		internal_page->numOfkeys++;
		internal_page->entries[insertion_index].pagenum = neighbor_page->leftmost_pagenum;
		end_point = neighbor_page->numOfkeys;

		buf_read_page(table_id, neighbor_page->leftmost_pagenum, (page_t*)child_page);
		child_page->parent_pagenum = internal_pagenum;
		buf_write_page(table_id, neighbor_page->leftmost_pagenum, (page_t*)child_page);

		for ( i = insertion_index + 1, j = 0; j < end_point; i++, j++ )
		{
			internal_page->entries[i].key = neighbor_page->entries[j].key;
			internal_page->entries[i].pagenum = neighbor_page->entries[j].pagenum;
			internal_page->numOfkeys++;

			// All children must now point up to the same parent.
			buf_read_page(table_id, neighbor_page->entries[j].pagenum, (page_t*)child_page);
			child_page->parent_pagenum = internal_pagenum;
		   	buf_write_page(table_id, neighbor_page->entries[j].pagenum, (page_t*)child_page);			
		}
		
		buf_write_page(table_id, internal_pagenum, (page_t*)internal_page);
  		buf_free_page(table_id, neighbor_pagenum);
		buf_pin_out(table_id, parent_pagenum);

		ret = db_delete_entry(table_id, parent_pagenum, k_prime);
	}
	else // neighbor_page - internal_page 순서
	{
		insertion_index = neighbor_page->numOfkeys;

		// Appen k_prime
		neighbor_page->entries[insertion_index].key = k_prime;
		neighbor_page->numOfkeys++;
		neighbor_page->entries[insertion_index].pagenum = internal_page->leftmost_pagenum;
		end_point = internal_page->numOfkeys;

		buf_read_page(table_id, internal_page->leftmost_pagenum, (page_t*)child_page);
		child_page->parent_pagenum = neighbor_pagenum;
		buf_write_page(table_id, internal_page->leftmost_pagenum, (page_t*)child_page);

		for ( i = insertion_index + 1, j = 0; j < end_point; i++, j++ )
		{
			neighbor_page->entries[i].key = internal_page->entries[j].key;
			neighbor_page->entries[i].pagenum = internal_page->entries[j].pagenum;
			neighbor_page->numOfkeys++;

			buf_read_page(table_id, internal_page->entries[j].pagenum, (page_t*)child_page);
			child_page->parent_pagenum = neighbor_pagenum;
			buf_write_page(table_id, internal_page->entries[j].pagenum, (page_t*)child_page);
		}

		buf_write_page(table_id, neighbor_pagenum, (page_t*)neighbor_page);
        buf_free_page(table_id, internal_pagenum);
		buf_pin_out(table_id, parent_pagenum);

		ret = db_delete_entry(table_id, parent_pagenum, k_prime);
	}

	free(internal_page);
    free(neighbor_page);
    free(child_page);
    free(parent_page);
	return ret;
}

int redistribute_leaf_pages(int64_t table_id, pagenum_t leaf_pagenum, pagenum_t neighbor_pagenum, int k_prime_index)
{
	Leaf_page* neighbor_page = (Leaf_page*)malloc(sizeof(Leaf_page));
    if ( neighbor_page == NULL ) return -1;
	Leaf_page* leaf_page = (Leaf_page*)malloc(sizeof(Leaf_page));
    if ( leaf_page == NULL ) return -1;

  	buf_read_page(table_id, neighbor_pagenum, (page_t*)neighbor_page);
 	buf_read_page(table_id, leaf_pagenum, (page_t*)leaf_page);

	Internal_page* parent_page = (Internal_page*)malloc(sizeof(Internal_page));
    if( parent_page == NULL ) return -1;
	pagenum_t parent_pagenum = leaf_page->parent_pagenum;
    buf_read_page(table_id, parent_pagenum, (page_t*)parent_page);

	Slot slot;
	int ret, i;
	char slotted_value[MAX_SIZE_OF_VALUE];
	char leaf_delivery[PAGE_SIZE] = {};
	char neighbor_delivery[PAGE_SIZE] = {};
	char temp_page[PAGE_SIZE] = {};
	char cleaner[MAX_SIZE_OF_VALUE] = {};
	
	uint64_t leaf_amount = leaf_page->free_space_amount;
	uint64_t neighbor_amount = neighbor_page->free_space_amount;
	uint32_t leaf_keys = leaf_page->numOfkeys;
	uint32_t neighbor_keys = neighbor_page->numOfkeys;
	uint16_t offset, temp_offset = PAGE_SIZE;

	memcpy(neighbor_delivery, neighbor_page, PAGE_SIZE);
	memcpy(leaf_delivery, leaf_page, PAGE_SIZE);
	
	if (parent_page->leftmost_pagenum != leaf_pagenum) // neighbor_page - leaf_page 순서.
	{
		while( leaf_amount >= THRESHOLD ) 
		{
			memmove(leaf_delivery+(SLOT_STARTING_POINT+SLOT_SIZE), leaf_delivery+SLOT_STARTING_POINT, SLOT_SIZE*leaf_keys);

			// 이웃 페이지의 가장 오른쪽 slot and val
			memcpy(&slot, neighbor_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(neighbor_keys-1)), SLOT_SIZE);
			memcpy(slotted_value, neighbor_delivery+slot.offset, slot.size);
			
			// cleaner
			memcpy(neighbor_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(neighbor_keys-1)), cleaner, SLOT_SIZE);
			memcpy(neighbor_delivery+slot.offset, cleaner, slot.size);
			
			// offset 계산.
			Slot temp_slot;
            memcpy(&temp_slot, leaf_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(leaf_keys-1)), SLOT_SIZE);

            offset = temp_slot.offset - slot.size;
            slot.offset = offset;
			
			// 맨 앞에 추가. 값도 위치에 넣기.
			memcpy(leaf_delivery+(SLOT_STARTING_POINT), &slot, SLOT_SIZE);
			memcpy(leaf_delivery+offset, slotted_value, slot.size);

			leaf_amount -= (SLOT_SIZE + slot.size);
			neighbor_amount += (SLOT_SIZE + slot.size);
			leaf_keys++;
			neighbor_keys--;
		}
		// leaf 페이지의 가장 왼쪽 slot
        memcpy(&slot, leaf_delivery+SLOT_STARTING_POINT, SLOT_SIZE);
        parent_page->entries[k_prime_index].key = slot.key;
	}
	else // leaf_page - neighbor_page 순서
	{
		while( leaf_amount >= THRESHOLD )
		{
			// 이웃 페이지의 가장 왼쪽 slot and val
			memcpy(&slot, neighbor_delivery+SLOT_STARTING_POINT, SLOT_SIZE);
			memcpy(slotted_value, neighbor_delivery+slot.offset, slot.size);

			// cleaner
			memcpy(neighbor_delivery+SLOT_STARTING_POINT, cleaner, SLOT_SIZE);
			memcpy(neighbor_delivery+slot.offset, cleaner,slot.size);
			
			// offset 계산.
			Slot temp_slot;
            memcpy(&temp_slot, neighbor_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*(neighbor_keys-1)), SLOT_SIZE);

            offset = temp_slot.offset - slot.size;
            slot.offset = offset;

			// leaf의 맨뒤에 slot and val추가.
			memcpy(leaf_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*leaf_keys), &slot, SLOT_SIZE);
			memcpy(leaf_delivery+offset, slotted_value, slot.size);

			memmove(neighbor_delivery+SLOT_STARTING_POINT, neighbor_delivery+SLOT_STARTING_POINT+SLOT_SIZE,SLOT_SIZE*(neighbor_keys-1));


			leaf_amount -= (SLOT_SIZE + slot.size);
   			neighbor_amount += (SLOT_SIZE + slot.size);
  			leaf_keys++;
   			neighbor_keys--;
		}
		// 이웃 페이지의 가장 왼쪽 slot
        memcpy(&slot, neighbor_delivery+SLOT_STARTING_POINT, SLOT_SIZE);
		parent_page->entries[k_prime_index].key = slot.key;
	}

	// neighbor_page compact
   	memcpy(temp_page, neighbor_delivery, PAGE_SIZE);
    memcpy(temp_page+SLOT_STARTING_POINT, cleaner, PAGE_SIZE-SLOT_STARTING_POINT);
 	for( i = 0; i < neighbor_keys; i++)
    {
    	memcpy(&slot, neighbor_delivery+(SLOT_STARTING_POINT+SLOT_SIZE*i), SLOT_SIZE);
      	memcpy(slotted_value, neighbor_delivery+slot.offset, slot.size);

       	temp_offset -= slot.size;
      	slot.offset = temp_offset;

        memcpy(temp_page+(SLOT_STARTING_POINT+SLOT_SIZE*i), &slot, SLOT_SIZE);
      	memcpy(temp_page+temp_offset, slotted_value, slot.size);
 	}

  	// move temp_page to in-memory page.
  	memcpy(neighbor_page, temp_page, PAGE_SIZE);
 	memcpy(leaf_page, leaf_delivery, PAGE_SIZE);

  	// update page header
  	neighbor_page->free_space_amount = neighbor_amount;
  	neighbor_page->numOfkeys = neighbor_keys;
    leaf_page->free_space_amount = leaf_amount;
   	leaf_page->numOfkeys = leaf_keys;

    // write
    buf_write_page(table_id, neighbor_pagenum, (page_t*)neighbor_page);
   	buf_write_page(table_id, leaf_pagenum, (page_t*)leaf_page);
	buf_write_page(table_id, parent_pagenum, (page_t*)parent_page);

	free(leaf_page);
    free(neighbor_page);
    free(parent_page);	
	return 0;
}

int redistribute_internal_pages(int64_t table_id, pagenum_t internal_pagenum, pagenum_t neighbor_pagenum, int k_prime_index, int k_prime, int min_keys)
{
	Internal_page* internal_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( internal_page == NULL ) return -1;
	Internal_page* neighbor_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( neighbor_page == NULL ) return -1;
	Internal_page* parent_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( parent_page == NULL ) return -1;
	Internal_page* child_page = (Internal_page*)malloc(sizeof(Internal_page));
    if ( child_page == NULL ) return -1;


	buf_read_page( table_id, internal_pagenum, (page_t*)internal_page);
  	buf_read_page( table_id, neighbor_pagenum, (page_t*)neighbor_page);

	pagenum_t parent_pagenum = internal_page->parent_pagenum;
	buf_read_page( table_id, parent_pagenum, (page_t*)parent_page);

	int i;
	if (parent_page->leftmost_pagenum != internal_pagenum ) // neighbor_page - internal_page  순서.
	{
		// move right one by one.
		for( i = internal_page->numOfkeys; i > 0; i-- )
		{
			internal_page->entries[i].key = internal_page->entries[i-1].key;
			internal_page->entries[i].pagenum = internal_page->entries[i-1].pagenum;
		}

		internal_page->entries[0].pagenum = internal_page->leftmost_pagenum;
		internal_page->leftmost_pagenum = neighbor_page->entries[neighbor_page->numOfkeys-1].pagenum;
			
		buf_read_page( table_id, neighbor_page->entries[neighbor_page->numOfkeys-1].pagenum, (page_t*)child_page);
		child_page->parent_pagenum = internal_pagenum;
		buf_write_page(table_id, neighbor_page->entries[neighbor_page->numOfkeys-1].pagenum, (page_t*)child_page); 

		internal_page->entries[0].key = k_prime;
		parent_page->entries[k_prime_index].key = neighbor_page->entries[neighbor_page->numOfkeys-1].key;
		neighbor_page->entries[neighbor_page->numOfkeys-1].pagenum = 0;
		
		internal_page->numOfkeys++;
		neighbor_page->numOfkeys--;
	}
	else // internal_page - neighbor_page 순서.
	{
		internal_page->entries[internal_page->numOfkeys].key = k_prime;
		internal_page->entries[internal_page->numOfkeys].pagenum = neighbor_page->leftmost_pagenum;

		buf_read_page(table_id, neighbor_page->leftmost_pagenum, (page_t*)child_page);
		child_page->parent_pagenum = internal_pagenum;
		buf_write_page(table_id, neighbor_page->leftmost_pagenum, (page_t*)child_page);

		parent_page->entries[k_prime_index].key = neighbor_page->entries[0].key;

		neighbor_page->leftmost_pagenum = neighbor_page->entries[0].pagenum;
		for( i = 0; i < neighbor_page->numOfkeys - 1; i++ )
		{
			neighbor_page->entries[i].key = neighbor_page->entries[i+1].key;
			neighbor_page->entries[i].pagenum = neighbor_page->entries[i+1].pagenum; 
		}

		internal_page->numOfkeys++;
        neighbor_page->numOfkeys--;
	}

	buf_write_page( table_id, internal_pagenum, (page_t*)internal_page);                                    
    buf_write_page( table_id, neighbor_pagenum, (page_t*)neighbor_page);
    buf_write_page( table_id, parent_pagenum, (page_t*)parent_page);

	free(internal_page);
    free(neighbor_page);
    free(parent_page);
    free(child_page);
	return 0;
}
