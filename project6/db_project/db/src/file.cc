#include "file.h"
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vector>
#include <map>
#include <stack>
#include <stdlib.h>
#include <string.h>
using namespace std;

vector<const char*> opened_files;
map<int64_t, int64_t> mapping_table; // key: table_id, value: file descriptor

int64_t file_open_table_file(const char* pathname)
{
	// firstly check whether the input file is already opened.
	// If the input file is already opened, connect the file.
	for( int i = 0; i < opened_files.size(); i++)
	{
		if ( strcmp(pathname, opened_files[i]) == 0 )
			return atoi(pathname+4);
	}

	int64_t fd = open(pathname, O_RDWR , 0777); 
	if ( fd == -1 ) 
    {
		fd = open(pathname, O_RDWR | O_CREAT | O_EXCL, 0777); 
		if ( fd == -1 )
        {
			cout << "File open error" << '\n';
           	return -1;
       	}
        if (ftruncate(fd, INITIAL_DB_FILE_SIZE) == -1) 
        {
   			cout << "File truncate error" << '\n';
     		return -1;
      	}

     	for(pagenum_t pagenum = 0; pagenum < INITIAL_DB_FILE_SIZE / PAGE_SIZE; pagenum++)
      	{
   			if ( pagenum == 0 ) 
         	{
				Header_page header_page;
				header_page.free_pagenum = (INITIAL_DB_FILE_SIZE / PAGE_SIZE)-1;
				header_page.numOfpages = (INITIAL_DB_FILE_SIZE / PAGE_SIZE);
          		if ( write(fd, &header_page, PAGE_SIZE ) != PAGE_SIZE )
                {
         			cout << "File write error" << '\n';
                   	return -1;
            	}
       		}
			else
			{
				Free_page free_page;
				free_page.next_free_pagenum = pagenum - 1;
          		if ( write(fd, &free_page, PAGE_SIZE ) != PAGE_SIZE )
             	{
              		cout << "File write error" << '\n';
              		return -1;
               	}
        	}
  		}
 	}

	int64_t table_id = atoi(pathname+4);;
	opened_files.push_back(pathname);
	mapping_table[table_id] = fd;       
	return table_id;
}

pagenum_t file_alloc_page(int64_t table_id)
{
	int64_t fd = mapping_table[table_id];
	Header_page header_page; 
	Free_page free_page;

	if( pread(fd, &header_page, PAGE_SIZE, 0) != PAGE_SIZE )
	{
   		cout << "File read error in alloc_page" << '\n';
      	return -1;
	}
	
	if ( header_page.free_pagenum == 0 )
	{
    	uint64_t before_size = header_page.numOfpages * PAGE_SIZE;
		uint64_t after_size = before_size * 2;
	
      	if( ftruncate(fd, after_size) == -1 )
      	{
          	cout << "File truncate error" << '\n';
          	return -1;
     	}

      	if ( lseek(fd, before_size, SEEK_SET) != before_size )
       	{
           	cout << "File lseek error" << '\n';
        	return -1;
    	}
		uint64_t new_size = after_size - before_size;
     	for( pagenum_t pagenum = 0; pagenum < new_size / PAGE_SIZE; pagenum++)
  		{
			if ( pagenum == 0 ) free_page.next_free_pagenum = 0;
			else free_page.next_free_pagenum = (new_size / PAGE_SIZE) + pagenum - 1;

       		if ( write(fd, &free_page, PAGE_SIZE ) != PAGE_SIZE )
            {
               	cout << "File write error" << '\n';
               	return -1;
      		}
		}
		header_page.free_pagenum = (after_size / PAGE_SIZE) - 1;
		header_page.numOfpages *= 2;
	}

	if ( pread( fd, &free_page, PAGE_SIZE, header_page.free_pagenum * PAGE_SIZE ) != PAGE_SIZE )
	{
		cout << "File read error in getting free_page" << '\n';
		return -1;
	}

	pagenum_t pagenum = header_page.free_pagenum;
	header_page.free_pagenum = free_page.next_free_pagenum;

	if ( pwrite(fd, &header_page, PAGE_SIZE, 0 ) != PAGE_SIZE )
	{
    	cout << "File write error" << '\n';
     	return -1;
 	}
	if ( fsync(fd) == -1 )
	{
		cout << "fsync error" << '\n';
		exit(1);
	}

	char cleaner[PAGE_SIZE] = {};
	
	if ( pwrite(fd, cleaner, PAGE_SIZE, pagenum * PAGE_SIZE) != PAGE_SIZE )
	{
		cout << "File write error" << '\n';
		return -1;
	}
	if ( fsync(fd) == -1 )
	{
		cout << "fsync error" << '\n';
		return -1;
	}

	return pagenum;   
}

void file_free_page(int64_t table_id, pagenum_t pagenum)
{
	int64_t fd = mapping_table[table_id];
	Header_page header_page;
	Free_page free_page;

	if( pread(fd, &header_page, PAGE_SIZE, 0) != PAGE_SIZE )
	{
		cout << "file read error" << '\n';
		exit(1);
	}

	free_page.next_free_pagenum = header_page.free_pagenum;
	header_page.free_pagenum = pagenum;

	if( pwrite(fd, &header_page, PAGE_SIZE, 0) != PAGE_SIZE )
	{
    	cout << "File write error" << '\n';
     	exit(1);
   	}
	if ( fsync(fd) == -1 )
	{
		cout << "fsync error" <<  '\n';
		exit(1);
	}

	if( pwrite(fd, &free_page, PAGE_SIZE, pagenum * PAGE_SIZE) != PAGE_SIZE )
	{
   		cout << "File write error" << '\n';
        exit(1);
  	}
	if ( fsync(fd) == -1 )
	{
		cout << "fsync error" << '\n';
		exit(1);
	}
}

void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest)
{
	int64_t fd = mapping_table[table_id];
	if( pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE) != PAGE_SIZE )
	{
		cout << "File read error in file_read_page" << '\n';
       	exit(1);
   	}
}

void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src)
{
	int64_t fd = mapping_table[table_id];
	if( pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE) != PAGE_SIZE )
	{
      	cout << "File write error" << '\n';
     	exit(1);
  	}

  	if ( fsync(fd) == -1 )
   	{
     	cout << "fsync error" << '\n';
      	exit(1);
  	}
}

void file_close_table_files()
{
	opened_files.clear();
	opened_files.shrink_to_fit();

	map<int64_t, int64_t>::iterator iter;
	for( iter = mapping_table.begin(); iter != mapping_table.end(); iter++ )
		close(iter->second);

	mapping_table.clear();
}

