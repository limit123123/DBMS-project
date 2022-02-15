#include "log.h"
#include "trx.h"
#include "bpt.h"
#include "page.h"
#include "file.h"
#include "buffer.h"
#include <iostream>

extern std::map<int, trx_t> trx_table;
extern int TRX_ID;
int64_t curr_LSN, flushedLSN;
const int64_t LOG_BUF_SIZE = 1000000;
char log_buf[LOG_BUF_SIZE];
pthread_mutex_t log_manager_latch = PTHREAD_MUTEX_INITIALIZER;
int log_fd;
FILE* fp;

void log_init_db(char* log_path, char* logmsg_path)
{
	log_fd = open(log_path, O_RDWR | O_CREAT | O_EXCL, 0777);
	fp = fopen(logmsg_path, "w");	
}

void make_LogRecord_1(int64_t last_LSN, int trx_id, int type)
{
	LogRecord1_t log_record;
	log_record.log_size = sizeof(log_record);
	log_record.LSN = curr_LSN;
	log_record.prev_LSN = last_LSN;
	log_record.trx_id = trx_id;
	log_record.type = type;

	if ((curr_LSN%LOG_BUF_SIZE) + log_record.log_size > LOG_BUF_SIZE) log_buf_force();

	memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE), &log_record, log_record.log_size);
	curr_LSN += log_record.log_size;
}

void make_LogRecord_2(int64_t last_LSN, int trx_id, int type, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t data_len, char* old_image, char* new_image)
{
	LogRecord2_t log_record;
	log_record.log_size = sizeof(log_record) + 2*data_len;
	if (type == COMPENSATE) log_record.log_size += 8;
	log_record.LSN = curr_LSN;
	log_record.prev_LSN = last_LSN;
	log_record.trx_id = trx_id;
	log_record.type = type;
	log_record.table_id = table_id;
	log_record.pagenum = pagenum;
	log_record.offset = offset;
	log_record.data_len = data_len;

	if ((curr_LSN%LOG_BUF_SIZE) + log_record.log_size > LOG_BUF_SIZE) log_buf_force();

	memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE), &log_record, 48);
	curr_LSN += 48;
	memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE)+48, old_image, data_len);
	curr_LSN += data_len;
	memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE)+data_len, new_image, data_len);
	curr_LSN += data_len;

	if (type == COMPENSATE) 
	{
		int64_t next_undo_LSN = find_next_undo_LSN(trx_id);
		memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE), &next_undo_LSN, sizeof(int64_t));
		curr_LSN += sizeof(int64_t);
	}
}

int64_t find_next_undo_LSN(int trx_id)
{
	int type;
	
	trx_t trx_obj = trx_table[trx_id];
	memcpy(&type, log_buf+trx_obj.last_LSN+24, 4);
	LogRecord2_t log_record;

	if (type == UPDATE)
	{		
		memcpy(&log_record, log_buf+trx_obj.last_LSN, sizeof(LogRecord2_t));
		return log_record.prev_LSN;
	}
	else if (type == COMPENSATE)
	{
		memcpy(&log_record, log_buf+trx_obj.last_LSN, sizeof(LogRecord2_t));
		
		int64_t prev_LSN;
		memcpy(&prev_LSN, log_buf+log_record.log_size-8, sizeof(int64_t));
		
		LogRecord2_t pred_log_record;
		memcpy(&pred_log_record, log_buf+prev_LSN, sizeof(LogRecord2_t));
		return pred_log_record.prev_LSN;
	}
}	


void log_buf_force()
{
	int offset = 0;
	int log_size = 0;
	
	while (1)
	{	
		memcpy(&log_size, log_buf+offset, sizeof(int));
		if (log_size == 0) break;
		pwrite(log_fd, log_buf+offset, log_size, flushedLSN);

		offset += log_size;
		flushedLSN += log_size;
	}

	memset(log_buf, 0, LOG_BUF_SIZE);
}

void roll_back(int trx_id)
{
	trx_t& trx_obj = trx_table[trx_id];

	int64_t tmp_LSN = trx_obj.last_LSN;
	LogRecord2_t log_record;
	int log_size;
	char delivery[PAGE_SIZE];

	while (tmp_LSN != -1)
	{
		memcpy(&log_size, log_buf+tmp_LSN, sizeof(int));
		memcpy(&log_record, log_buf+tmp_LSN, log_size);
		
		if (log_record.type == UPDATE)
		{
			char inv_old_image[MAX_SIZE_OF_VALUE];
			char inv_new_image[MAX_SIZE_OF_VALUE];
			memcpy(inv_new_image, log_buf+tmp_LSN+48, log_record.data_len);
			memcpy(inv_old_image, log_buf+tmp_LSN+48+log_record.data_len, log_record.data_len);
			
			int64_t table_id = log_record.table_id;
			pagenum_t pagenum = log_record.pagenum;
			uint16_t offset = log_record.offset;
			uint16_t data_len = log_record.data_len;


			trx_obj.last_LSN = curr_LSN;
			make_LogRecord_2(trx_obj.last_LSN, trx_id, COMPENSATE, table_id, pagenum, offset, data_len, inv_new_image, inv_old_image);			

			buf_read_page(table_id, pagenum, (page_t*)delivery);
			memcpy(delivery+offset, inv_new_image, data_len);
			buf_write_page(table_id, pagenum, (page_t*)delivery);
		}

		tmp_LSN = log_record.prev_LSN;
	}

	make_LogRecord_1(trx_obj.last_LSN, trx_id, ROLLBACK);
}

std::list<int> analysis_pass()
{
	std::list<int> winners, losers;
	fprintf(fp, "[ANALYSIS] Analysis pass start\n");

	LogRecord1_t log_record;
	uint64_t offset = 0;
	int log_size = 0;
	int max_trx_id = 1;
	while (1)
	{
		pread(log_fd, &log_size, sizeof(int), offset);
		if (log_size == 0) break;
		
		pread(log_fd, &log_record, sizeof(log_record), offset);

		if (log_record.type == BEGIN)
		{
			losers.push_back(log_record.trx_id);
		}
		else if (log_record.type == COMMIT || log_record.type == ROLLBACK)
		{
			losers.remove(log_record.trx_id);
			winners.push_back(log_record.trx_id);
		}

		if (log_record.trx_id > max_trx_id)
			max_trx_id = log_record.trx_id;

		if (log_record.LSN > flushedLSN)
			curr_LSN = log_record.LSN + log_record.log_size;

		offset += log_size;
	}

	TRX_ID = max_trx_id;
	flushedLSN = curr_LSN;

	
	//winner.sort();
	//loser.sort();

	fprintf(fp, "[ANALYSIS] Analysis success. Winner:");
	for (int winner_id : winners)
		fprintf(fp, " %d", winner_id);

	fprintf(fp, ", Loser:");
	for (int loser_id : losers)
		fprintf(fp, " %d", loser_id);

	fprintf(fp, "\n");

	return losers;
}


	
void redo_pass(int flag, int log_num)
{
	fprintf(fp, "[REDO] Redo pass start\n");
	
	LogRecord1_t log_record;
	LogRecord2_t log_record2;
	uint64_t offset = 0;
	int log_size = 0;
	char filename[20];
	char delivery[PAGE_SIZE];
	int64_t page_LSN;

	while (1)
	{
		if (log_num == 0) break;
		if (flag == 1) log_num--;


		pread(log_fd, &log_size, sizeof(int), offset);
		if (log_size == 0) break;
		
		pread(log_fd, &log_record, sizeof(log_record), offset);

		if (log_record.type == BEGIN)
		{
			fprintf(fp, "LSN %lu [BEGIN] Transaction id %d\n", log_record.LSN, log_record.trx_id);
		}
		else if (log_record.type == COMMIT)
		{
			fprintf(fp, "LSN %lu [COMMIT] Transaction id %d\n", log_record.LSN, log_record.trx_id);
		}		
		else if (log_record.type == ROLLBACK)
		{
			fprintf(fp, "LSN %lu [ROLLBACK] Transaction id %d\n", log_record.LSN, log_record.trx_id);
		}
		else 
		{	
			pread(log_fd, &log_record2, sizeof(log_record2), offset);

			sprintf(filename, "DATA%lu", log_record2.table_id);
			file_open_table_file(filename);	
			buf_read_page(log_record2.table_id, log_record2.pagenum, (page_t*)delivery);
			memcpy(&page_LSN, delivery+24, 8);  

			// redo write only log.LSN > page_LSN
			if (log_record2.LSN > page_LSN) 
			{
				uint16_t data_len = log_record2.data_len;
				char new_image[MAX_SIZE_OF_VALUE];
				pread(log_fd, new_image, offset+48+data_len, data_len);
				
				memcpy(delivery+log_record2.offset, new_image, data_len);
				memcpy(delivery+24, &log_record2.LSN, 8);

				buf_write_page(log_record2.table_id, log_record2.pagenum, (page_t*)delivery);

				if (log_record2.type == UPDATE)
					fprintf(fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", log_record2.LSN, log_record2.trx_id);
				else if (log_record2.type == COMPENSATE)
				{
					uint64_t next_undo_lsn;
					pread(log_fd, &next_undo_lsn, offset+48+(2*data_len), 8);
					fprintf(fp, "LSN %lu [CLR] next redo lsn %lu\n", log_record2.LSN, next_undo_lsn);
				}
			}
			else // consider redo! (just skip)
			{
				fprintf(fp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log_record.LSN, log_record.trx_id);
				buf_page_latch_release(log_record2.table_id, log_record2.pagenum);
			}
		}

		offset += log_size;
	}


	if (flag != 1) fprintf(fp, "[REDO] Redo pass end\n");
}


void undo_pass(std::list<int> losers, int flag, int log_num)
{
	fprintf(fp, "[UNDO] Undo pass start\n");

	std::map<int, int64_t> last_SN; // trx_id, LSN
	std::map<int, int64_t> next_undo_SN; // trx_id, LSN;
	LogRecord1_t log_record;
	LogRecord2_t log_record2;
	uint64_t undo_LSN_offset = 0;
	int log_size = 0;
	char delivery[PAGE_SIZE];
	uint64_t offset = 0;
	std::list<int64_t> losers_action_LSN;
	while (1)
	{
		pread(log_fd, &log_size, sizeof(int), offset);
		if (log_size == 0) break;
		
		pread(log_fd, &log_record, sizeof(log_record), offset);

		std::list<int>::iterator iter;

		if (log_record.type == BEGIN || log_record.type == UPDATE || log_record.type == COMPENSATE)
		{
			iter = std::find(losers.begin(), losers.end(), log_record.trx_id);
			if (iter != losers.end())
			{
				last_SN[log_record.trx_id] = log_record.LSN;
				next_undo_SN[log_record.trx_id] = log_record.LSN;
			}
		}
		offset += log_size;
	}
	

	while (!losers.empty())
	{
		if (log_num == 0) break;
		if (flag == 2) log_num--;

		undo_LSN_offset = 0;
		std::map<int, int64_t>::iterator iter2;
		for (iter2 = next_undo_SN.begin(); iter2 != next_undo_SN.end(); iter2++)
			if (iter2->second > undo_LSN_offset) undo_LSN_offset = iter2->second;


		pread(log_fd, &log_size, sizeof(int), undo_LSN_offset);
		pread(log_fd, &log_record2, sizeof(log_record2), undo_LSN_offset);


		if (log_record2.type == BEGIN)
		{
			make_LogRecord_1(last_SN[log_record2.trx_id], log_record2.trx_id, ROLLBACK);
			losers.remove(log_record2.trx_id);
			last_SN.erase(log_record2.trx_id);
			next_undo_SN.erase(log_record2.trx_id);
		}
		else if (log_record2.type == UPDATE)
		{
			uint16_t data_len = log_record2.data_len;
			char old_image[MAX_SIZE_OF_VALUE];
			char new_image[MAX_SIZE_OF_VALUE];
			pread(log_fd, old_image, undo_LSN_offset+48, data_len);
			pread(log_fd, new_image, undo_LSN_offset+48+data_len, data_len);
		

			LogRecord2_t new_log_record;
			new_log_record.log_size = sizeof(new_log_record) + 2*data_len+8;
			new_log_record.LSN = curr_LSN;
			new_log_record.prev_LSN = last_SN[log_record2.trx_id];
			new_log_record.trx_id = log_record2.trx_id;
			new_log_record.type = COMPENSATE;
			new_log_record.table_id = log_record2.table_id;
			new_log_record.pagenum = log_record2.pagenum;
			new_log_record.offset = log_record2.offset;
			new_log_record.data_len = log_record2.data_len;


			last_SN[log_record2.trx_id] = curr_LSN;
			


			pthread_mutex_lock(&log_manager_latch);
		
			if ((curr_LSN%LOG_BUF_SIZE) + log_record.log_size > LOG_BUF_SIZE) log_buf_force();

			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE), &new_log_record, 48);
			curr_LSN += 48;
			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE)+48, new_image, data_len);
			curr_LSN += data_len;
			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE)+48+data_len, old_image, data_len);
			curr_LSN += data_len;
			

			int64_t next_undo_LSN = log_record2.prev_LSN;
			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE), &next_undo_LSN, sizeof(int64_t));
			curr_LSN += sizeof(int64_t);
			
			next_undo_SN[log_record2.trx_id] = next_undo_LSN;
			pthread_mutex_unlock(&log_manager_latch);
		

			buf_read_page(log_record2.table_id, log_record2.pagenum, (page_t*)delivery);
			memcpy(delivery+log_record2.offset, old_image, data_len);
			memcpy(delivery+24, &curr_LSN, 8);
			buf_write_page(log_record2.table_id, log_record2.pagenum, (page_t*)delivery);

			fprintf(fp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", log_record2.LSN, log_record2.trx_id);
		}
		else if (log_record2.type == COMPENSATE)
		{
			int log_size3;
			int64_t curr_undo_LSN;
			pread(log_fd, &curr_undo_LSN, undo_LSN_offset+48+(2*log_record2.data_len), sizeof(int64_t));
		
			LogRecord2_t log_record3;
			pread(log_fd, &log_size3, sizeof(int), curr_undo_LSN);
			pread(log_fd, &log_record3, sizeof(log_record3), curr_undo_LSN);

			uint16_t data_len = log_record3.data_len;
			char old_image[MAX_SIZE_OF_VALUE];
			char new_image[MAX_SIZE_OF_VALUE];

			pread(log_fd, old_image, curr_undo_LSN+48, data_len);
			pread(log_fd, new_image, curr_undo_LSN+48+data_len, data_len);
		

			LogRecord2_t new_log_record;
			new_log_record.log_size = sizeof(new_log_record) + 2*data_len+8;
			new_log_record.LSN = curr_LSN;
			new_log_record.prev_LSN = last_SN[log_record2.trx_id];
			new_log_record.trx_id = log_record3.trx_id;
			new_log_record.type = COMPENSATE;
			new_log_record.table_id = log_record3.table_id;
			new_log_record.pagenum = log_record3.pagenum;
			new_log_record.offset = log_record3.offset;
			new_log_record.data_len = log_record3.data_len;

			last_SN[log_record3.trx_id] = curr_LSN;

			
			pthread_mutex_lock(&log_manager_latch);
			if ((curr_LSN%LOG_BUF_SIZE) + log_record.log_size > LOG_BUF_SIZE) log_buf_force();

			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE), &new_log_record, 48);
			curr_LSN += 48;
			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE)+48, new_image, data_len);
			curr_LSN += data_len;
			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE)+48+data_len, old_image, data_len);
			curr_LSN += data_len;
			

			int64_t next_undo_LSN = log_record3.prev_LSN;
			memcpy(log_buf+(curr_LSN%LOG_BUF_SIZE), &next_undo_LSN, sizeof(int64_t));
			curr_LSN += sizeof(int64_t);
			
			next_undo_SN[log_record2.trx_id] = next_undo_LSN;
			pthread_mutex_unlock(&log_manager_latch);

			fprintf(fp, "LSN %lu [CLR] next undo lsn %lu\n", log_record2.LSN, next_undo_LSN);
		}
	}


	if (flag != 2) fprintf(fp, "[UNDO] Undo pass end\n");
}
