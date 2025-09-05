/*
* Copyright JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#ifndef __LRU_BIT_H
#define __LRU_BIT_H

#include <string.h>
#include <stdlib.h>
#include <strings.h>
#include "algorithm/bitsop.h"
#include "timer/timer_list.h"
#include "log/logger.h"
#include "raw_data.h"
#include "data_chunk.h"
#include "table/hotbackup_table_def.h"
#include "sys_malloc.h"

#define IDX_SIZE (4 << 10) //4K
#define BLK_SIZE (256 << 10) //256K
#define LRU_BITS (2 << 10) //2k

#define BBLK_OFF(v) (v >> 21)
#define IDX_BYTE_OFF(v) ((v >> 9) & 0xFFF)
#define IDX_BYTE_SHIFT(v) ((v >> 6) & 0x7)
#define BLK_8_BYTE_OFF(v) ((v >> 6) & 0x7FFF)
#define BLK_BYTE_OFF(v) ((v >> 3) & 0x3FFFF)
#define BLK_BYTE_SHIFT(v) (v & 0x7)

/*
 *  Node ID bitmap storage table
 *
 *====================================================================================
 *| 11 b      | 12 b         | 3 b           | 3 b                 |     3 b       | 
 *| bblk off  | idx byte off | idx byte shift| 
 *            |        blk  8-bytes off      |
 *            |                   blk byte off                     | blk byte shift|
 *====================================================================================
 */

typedef struct lru_bit {
	char _idx[IDX_SIZE];
	char _blk[BLK_SIZE];

	lru_bit()
	{
		bzero(_idx, sizeof(_idx));
		bzero(_blk, sizeof(_blk));
	}

	~lru_bit()
	{
	}

	/* returns 1 if set hit, otherwise returns 0 */
	int set(unsigned int v, int b)
	{
		int hit = 0;
		uint32_t byte_shift = BLK_BYTE_SHIFT(v);
		uint32_t byte_offset = BLK_BYTE_OFF(v);

		if (b) {
			if (ISSET_B(byte_shift, _blk + byte_offset)) {
				hit = 1;
			} else {
				SET_B(byte_shift, _blk + byte_offset);
				SET_B(IDX_BYTE_SHIFT(v),
				      _idx + IDX_BYTE_OFF(v));
			}
		} else {
			CLR_B(byte_shift, _blk + byte_offset);
		}

		return hit;
	}

	/* return total clear bits */
	int clear(int idx_off)
	{
		int clear_bits = COUNT_B(_blk + (idx_off << 6), 1 << 6);

		/* 1 byte idx */
		memset(_idx + idx_off, 0x00, 1);

		/* 64 bytes blk */
		memset(_blk + (idx_off << 6), 0x00, 1 << 6);

		return clear_bits;
	}

	uint64_t read(int idx_off, int idx_shift)
	{
		unsigned char *ix = (unsigned char *)_idx + idx_off;

		if (ISSET_B(idx_shift, ix)) {
			uint64_t *p = (uint64_t *)_blk;
			return p[(idx_off << 3) + idx_shift];
		} else {
			return 0;
		}
	}
} lru_bit_t;

/* 
 *
 * Scan frequency and speed control
 * 1. Must not affect normal update synchronization
 * 2. Try to complete one round of scanning within the controlled time
 *
 */
#define LRU_SCAN_STOP_UNTIL 20 //20
#define LRU_SCAN_INTERVAL 10 //10ms

class RawData;
class LruWriter {
    public:
	LruWriter(BinlogWriter *);
	virtual ~LruWriter();

	int do_init();
	int Write(unsigned int id);
	int Commit(void);

    private:
	BinlogWriter *_log_writer;
	RawData *_raw_data;
};

class LruBitUnit;
class LruBitObj : private TimerObject {
    public:
	LruBitObj(LruBitUnit *);
	~LruBitObj();

	int do_init(BinlogWriter *, int stop_until = LRU_SCAN_STOP_UNTIL);
	int SetNodeID(unsigned int v, int b);

    private:
	int Scan(void);
	virtual void job_timer_procedure(void);

    private:
	lru_bit_t *_lru_bits[LRU_BITS];
	uint16_t _max_lru_bit;
	uint16_t _scan_lru_bit;
	uint16_t _scan_idx_off;
	uint16_t _scan_stop_until;

	LruWriter *_lru_writer;
	LruBitUnit *_owner;

	friend class LruBitUnit;

    private:
	/* statistic */
	uint32_t scan_tm;
	StatCounter lru_scan_tm;

	StatCounter total_bits;
	StatCounter total_1_bits;

	StatCounter lru_set_count;
	StatCounter lru_set_hit_count;
	StatCounter lru_clr_count;
};

class LruBitUnit {
    public:
	LruBitUnit(TimerUnit *);
	~LruBitUnit();

	int do_init(BinlogWriter *);
	void enable_log(void);
	void disable_log(void);
	int check_status()
	{
		return _is_start;
	} // 0: not started, 1: started
	int Set(unsigned int v);
	int Unset(unsigned int v);

    private:
	TimerList *_scan_timerlist;
	LruBitObj *_lru_bit_obj;
	int _is_start;
	TimerUnit *_owner;

	friend class LruBitObj;
};

#endif
