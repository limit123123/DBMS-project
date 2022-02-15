#include "file.h"
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
 * Tests file open/close APIs.
 * 1. Open a file and check the descriptor
 * 2. Check if the file's initial size is 10 MiB
 */
TEST(FileInitTest, HandlesInitialization) {
	int table_id;              
	
	std::string pathname = "test.txt";  // customize it to your test file

	// Open a database file
 	table_id = file_open_table_file(pathname.c_str());
	
	// Check if the file is opened
  	ASSERT_TRUE(table_id >= 0)  // change the condition to your design's behavior
		<< "File open error";

	Header_page* header_page = (Header_page*)malloc(sizeof(Header_page));
	file_read_page(table_id, 0, (page_t*)header_page);

  	// Check the size of the initial file
	/* fetch the number of pages from the header page */

  	EXPECT_EQ(header_page->numOfpages, INITIAL_DB_FILE_SIZE / PAGE_SIZE);

  	// Close all database files
  	file_close_table_files();

  	// Remove the db file
  	int is_removed = remove(pathname.c_str());

  	ASSERT_EQ(is_removed, 0);
	free(header_page);
}

/*
 * TestFixture for page allocation/deallocation tests
 */
class FileTest : public ::testing::Test {
 protected:
  	/*
   	* NOTE: You can also use constructor/destructor instead of SetUp() and
   	* TearDown(). The official document says that the former is actually
   	* perferred due to some reasons. Checkout the document for the difference
   	*/
  	FileTest() { table_id = file_open_table_file(pathname.c_str()); }

	 ~FileTest() {
		if (table_id >= 0) {
      			file_close_table_files();
    		}
  	}	

  	int table_id;                // file descriptor
  	std::string pathname = "test.txt";  // path for the file
};

/*
 * Tests page allocation and free
 * 1. Allocate 2 pages and free one of them, traverse the free page list
 *    and check the existence/absence of the freed/allocated page
 */
TEST_F(FileTest, HandlesPageAllocation) {
	pagenum_t allocated_page, freed_page;

	// Allocate the pages
	allocated_page = file_alloc_page(table_id);
	freed_page = file_alloc_page(table_id);

	// Free one page
  	file_free_page(table_id, freed_page);

	// Traverse the free page list and check the existence of the freed/allocated
        // pages. You might need to open a few APIs soley for testing.

	
	Header_page* header_page = (Header_page*)malloc(sizeof(Header_page));
	file_read_page(table_id, 0, (page_t*)header_page);

	EXPECT_EQ(header_page->free_pagenum, freed_page);

	free(header_page);
}

/*
 * Tests page read/write operations
 * 1. Write/Read a page with some random content and check if the data matches
 */
TEST_F(FileTest, CheckReadWriteOperation) {

	pagenum_t pagenum = file_alloc_page(table_id);
	
	page_t src, dest;
	memset(src.reserved, 'a', PAGE_SIZE);
	
	file_write_page(table_id, pagenum, &src);
	file_read_page(table_id, pagenum, &dest);

	EXPECT_EQ(memcmp(&src, &dest, PAGE_SIZE), 0)
		<< "read/write operation error";
}


TEST_F(FileTest, CheckDoubling)
{

		const int COUNT = 3;

        Header_page* header_page = (Header_page*)malloc(sizeof(Header_page));
        file_read_page(table_id, 0, (page_t*)header_page);

        pagenum_t totalpage = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
        pagenum_t freepages; 
        for(int i = 1; i <= COUNT; i++ )
        {
                freepages = header_page->free_pagenum;
                for(pagenum_t num = 0; num <= freepages; num++)
                        file_alloc_page(table_id);
                totalpage *= 2;
                file_read_page(table_id, 0, (page_t*)header_page);
                EXPECT_EQ(header_page->numOfpages, totalpage);
        }
}

