/*
* Copyright [2021] JD.com, Inc.
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

#include "lru_bit.h"
#include "mem_check.h"
#include "table/hotbackup_table_def.h"
#include "field/field.h"
#include "table/table_def.h"
#include "node.h"
#include "node_index.h"
#include "log/log.h"
#include "table/table_def_manager.h"

LruBitObj::LruBitObj(LruBitUnit *t)
	: _max_lru_bit(0), _scan_lru_bit(0), _scan_idx_off(0), _lru_writer(0),
	  _owner(t), scan_tm(0)

{
	bzero(_lru_bits, LRU_BITS * sizeof(lru_bit_t *));

	lru_scan_tm = g_stat_mgr.get_stat_int_counter(HBP_LRU_SCAN_TM);
	total_bits = g_stat_mgr.get_stat_int_counter(HBP_LRU_TOTAL_BITS);
	total_1_bits = g_stat_mgr.get_stat_int_counter(HBP_LRU_TOTAL_1_BITS);
	lru_set_count = g_stat_mgr.get_stat_int_counter(HBP_LRU_SET_COUNT);
	lru_clr_count = g_stat_mgr.get_stat_int_counter(HBP_LRU_CLR_COUNT);
	lru_set_hit_count =
		g_stat_mgr.get_stat_int_counter(HBP_LRU_SET_HIT_COUNT);
}

LruBitObj::~LruBitObj()
{
	for (int i = 0; i < LRU_BITS; i++) {
		DELETE(_lru_bits[i]);
	}
}

int LruBitObj::SetNodeID(unsigned int v, int b)
{
	int off = BBLK_OFF(v);

	if (!_lru_bits[off]) {
		NEW(lru_bit_t, _lru_bits[off]);
		if (!_lru_bits[off])
			return -1;
	}

	/* stat set/clr count */
	if (b)
		lru_set_count++;
	else
		lru_clr_count++;

	if (_lru_bits[off]->set(v, b)) {
		/* stat set hit count */
		lru_set_hit_count++;
	} else if (b) {
		total_1_bits++;
	}

	_max_lru_bit < off ? _max_lru_bit = off : off;

	/* stat total bits */
	total_bits < v ? total_bits = v : total_bits;

	return 0;
}

void LruBitObj::job_timer_procedure(void)
{
	log4cplus_debug("enter timer procedure");
	Scan();
	attach_timer(_owner->_scan_timerlist);
	log4cplus_debug("leave timer procedure");
}

int LruBitObj::do_init(BinlogWriter *w, int stop_until)
{
	_scan_stop_until = stop_until;

	NEW(LruWriter(w), _lru_writer);
	if (!_lru_writer)
		return -1;

	if (_lru_writer->do_init())
		return -1;

	return 0;
}

int LruBitObj::Scan(void)
{
	if (scan_tm == 0) {
		INIT_MSEC(scan_tm);
	}

	lru_bit_t *p = _lru_bits[_scan_lru_bit];
	if (!p)
		return 0;

	unsigned found_id = 0;
	for (; _scan_idx_off < IDX_SIZE;) {
		unsigned found = 0;

		//扫描idx中的1 byte, 最大会有512个node id
		for (int j = 0; j < 8; ++j) {
			//读取idx中的第_scan_idx_off个字节的第j位对应的blk中的8 bytes
			uint64_t v = p->read(_scan_idx_off, j);
			if (0 == v)
				continue;

			//扫描blk中的8 bytes
			for (int i = 0; i < 64; ++i) {
				if (v & 0x1) {
					found += 1;

					uint32_t id = (_scan_lru_bit << 21) +
						      (_scan_idx_off << 9) +
						      (j << 6) + i;

					log4cplus_debug(
						"adjust lru: node-id=%u", id);

					_lru_writer->Write(id);
				}

				v >>= 1;
			}
		}

		if (found > 0) {
			//批量写入lru变更
			_lru_writer->Commit();

			//idx清零1byete， blk清零64bytes
			total_1_bits -= p->clear(_scan_idx_off);
		}

		_scan_idx_off += 1;

		found_id += found;
		// 如果超过此水位，终止扫描, 等待下一次被调度
		if (found_id >= _scan_stop_until) {
			return 0;
		}
	}

	//调整为下一个lru_bit(4k)
	_scan_idx_off = 0;
	_scan_lru_bit += 1;
	if (_scan_lru_bit > _max_lru_bit) {
		_scan_lru_bit = 0;

		CALC_MSEC(scan_tm);
		lru_scan_tm = scan_tm;
		scan_tm = 0;
	}

	return 0;
}

LruBitUnit::LruBitUnit(TimerUnit *p)
	: _scan_timerlist(0), _lru_bit_obj(0), _is_start(0), _owner(p)
{
}

LruBitUnit::~LruBitUnit()
{
	DELETE(_lru_bit_obj);
}

int LruBitUnit::do_init(BinlogWriter *w)
{
	_scan_timerlist =
		_owner->get_timer_list_by_m_seconds(LRU_SCAN_INTERVAL);

	NEW(LruBitObj(this), _lru_bit_obj);
	if (!_lru_bit_obj)
		return -1;

	if (_lru_bit_obj->do_init(w))
		return -1;

	return 0;
}

void LruBitUnit::enable_log(void)
{
	_is_start = 1;
	_lru_bit_obj->attach_timer(_scan_timerlist);
}

void LruBitUnit::disable_log(void)
{
	_is_start = 0;
	_lru_bit_obj->disable_timer();
}

int LruBitUnit::Set(unsigned int v)
{
	return _is_start ? _lru_bit_obj->SetNodeID(v, 1) : 0;
}

int LruBitUnit::Unset(unsigned int v)
{
	return _is_start ? _lru_bit_obj->SetNodeID(v, 0) : 0;
}

LruWriter::LruWriter(BinlogWriter *w) : _log_writer(w), _raw_data(0)
{
}

LruWriter::~LruWriter()
{
	DELETE(_raw_data);
}

int LruWriter::do_init()
{
	NEW(RawData(&g_stSysMalloc, 1), _raw_data);
	if (!_raw_data)
		return -1;

	unsigned type = DTCHotBackup::SYNC_LRU;
	if (_raw_data->init(0,
			    TableDefinitionManager::instance()
				    ->get_hot_backup_table_def()
				    ->key_size(),
			    (const char *)&type, 0, -1, -1, 0))
		return -1;

	return 0;
}

int LruWriter::Write(unsigned int v)
{
	log4cplus_debug("enter LruWriter, lru changes, node id:%u", v);

	Node node = I_SEARCH(v);
	if (!node) //NODE已经不存在，不处理
		return 0;

	DataChunk *p = M_POINTER(DataChunk, node.vd_handle());
	RowValue r(
		TableDefinitionManager::instance()->get_hot_backup_table_def());

	r[0].u64 = DTCHotBackup::SYNC_LRU;
	r[1].u64 = DTCHotBackup::NON_VALUE;

	//self table-definition encode packed key
	r[2] = TableDefinitionManager::instance()
		       ->get_cur_table_def()
		       ->packed_key(p->key());
	r[3].Set(0);

	return _raw_data->insert_row(r, false, false);
}

int LruWriter::Commit(void)
{
	log4cplus_debug("lru write commit");

	_log_writer->insert_header(BINLOG_LRU, 0, 1);
	_log_writer->append_body(_raw_data->get_addr(), _raw_data->data_size());

	log4cplus_debug("body: len=%d, content:%x", _raw_data->data_size(),
			*(char *)_raw_data->get_addr());

	_raw_data->delete_all_rows();
	return _log_writer->Commit();
}
