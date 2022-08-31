/*
 * =====================================================================================
 *
 *       Filename:  global.h
 *
 *    Description:  global class definition.
 *
 *        Version:  1.0
 *        Created:  11/01/2021
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  chenyujie, chenyujie28@jd.com@jd.com
 *        Company:  JD.com, Inc.
 *
 * =====================================================================================
 */

#ifndef __HB_GLOBAL_H
#define __HB_GLOBAL_H

/* output u64 format */
#if __WORDSIZE == 64
# define UINT64FMT "%lu"
#else
# define UINT64FMT "%llu"
#endif

#define MAX_ASYNC_FILE_SIZE         (10<<20)	//10M
#define ASYNC_FILE_CONTROLLER_SIZE  512	//512bytes
#define ASYNC_FILE_PATH             "../log/hwc"
#define ASYNC_FILE_CONTROLLER       ASYNC_FILE_PATH"/controller"
#define ASYNC_FILE_NAME             ASYNC_FILE_PATH"/hwc_"
#define ASYNC_FILE_END_FLAG         0xFFFFFFFFUL
#define ASYNC_WRITER_MAP_FILES      0x3

#define READER_SLEEP_TIME           500	//500ms
#define READER_RETRY_COUNT          20

#define SYS_CONFIG_FILE             "/etc/dtc/hbp.conf"
/*
 *   err code
 */
class CHBGlobal {
public:
	enum {

		ASYNC_PROCESS_OK = 0,
		ASYNC_PROCESS_ERR = -1,

	};

	enum {

		ASYNC_NEED_SWTICH_FILE = -10,
		ASYNC_READER_WAIT_DATA = -11,
	};

	enum {

		ERR_ASYNC_WRITER_OVERFLOW = -20,
		ERR_ASYNC_READER_OVERFLOW = -21,
		ERR_ASYNC_READER_POS_ERR = -22,
		ERR_ASYNC_WRITER_POS_ERR = -23,
		ERR_ASYNC_CONTROLLER_ERR = -24,
		ERR_ASYNC_WRITER_LOGIC = -25,
		ERR_ASYNC_READER_LOGIC = -26,
		ERR_ASYNC_SWTICH_FILE_ERR = -27,
		ERR_FULL_SYNC_NOT_COMPLETE = -28,
	};

	//type
	enum {

		SYNC_LRU = 1,
		SYNC_INSERT = 2,
		SYNC_UPDATE = 4,
		SYNC_PURGE = 8,
		SYNC_DELETE = 16,
		SYNC_CLEAR = 32,
		SYNC_COLEXPAND = 64,
		SYNC_COLEXPAND_CMD = 128,
	};
};
#endif
