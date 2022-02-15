#ifndef __LOG_H_
#define __LOG_H_

#include "bpt.h"
#include "buffer.h"
#include "page.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>
#include <cstdio>
#include <map>
#include <list>
#include <stack>
#include <algorithm>


#define BEGIN 0
#define UPDATE 1
#define COMMIT 2
#define ROLLBACK 3
#define COMPENSATE 4

typedef struct LogRecord1_t
{
	int log_size;
	int64_t LSN;
	int64_t prev_LSN;
	int trx_id;
	int type;
} __attribute__((packed)) LogRecord1_t;

typedef struct LogRecord2_t
{
	int log_size;
	int64_t LSN;
	int64_t prev_LSN;
	int trx_id;
	int type;
	int64_t table_id;
	pagenum_t pagenum;
	uint16_t offset;
	uint16_t data_len;
} __attribute__((packed)) LogRecord2_t;


void log_init_db(char* log_path, char* logmsg_path);
void make_LogRecord_1(int64_t last_LSN, int trx_id, int type);
void make_LogRecord_2(int64_t last_LSN, int trx_id, int type, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t data_len, char* old_image, char* new_image);
int64_t find_next_undo_LSN(int trx_id);
void log_buf_force();
void roll_back(int trx_id);
std::list<int> analysis_pass();
void redo_pass(int flag, int log_num);
void undo_pass(std::list<int> losers, int flag, int log_num);

#endif /* __LOG_H_ */
