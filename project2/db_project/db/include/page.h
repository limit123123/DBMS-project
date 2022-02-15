#ifndef __PAGE_H__
#define __PAGE_H__

#include <stdint.h>

#define INITIAL_DB_FILE_SIZE (1024*1024*10)
#define PAGE_SIZE (1024*4)
#define INITIAL_FREE_SPACE_AMOUNT 3968

typedef uint64_t pagenum_t;
struct page_t
{
	// in-memory page structure 
	char reserved[PAGE_SIZE];
};


// Add any structures you need
typedef struct
{
	pagenum_t free_pagenum;
	pagenum_t numOfpages;
	pagenum_t root_pagenum;
	char reserved[4072];
} __attribute__((packed)) Header_page;

typedef struct
{
	pagenum_t next_free_pagenum;
	char not_used[4088];
} __attribute__((packed)) Free_page;

typedef struct
{
	pagenum_t parent_pagenum;
	uint32_t is_leaf = 1;
	uint32_t numOfkeys = 0;
	char reserved[96];
	uint64_t free_space_amount = INITIAL_FREE_SPACE_AMOUNT;
	pagenum_t sibling_pagenum;
	char free_space[INITIAL_FREE_SPACE_AMOUNT];
} __attribute__((packed)) Leaf_page;

typedef struct
{
	uint64_t key;
	uint16_t size;
	uint16_t offset;
} __attribute__((packed)) Slot;

typedef struct
{
	uint64_t key;
	pagenum_t pagenum;
} __attribute__((packed)) Entry;

typedef struct
{
	pagenum_t parent_pagenum;
	uint32_t is_leaf = 0;
	uint32_t numOfkeys = 0;
	char reserved[104];
	pagenum_t leftmost_pagenum;
	Entry entries[248];
} __attribute__((packed)) Internal_page;

#endif /*__PAGE_H__*/
