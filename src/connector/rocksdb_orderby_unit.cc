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
* 
*/

#include "rocksdb_orderby_unit.h"

RocksdbOrderByUnit::RocksdbOrderByUnit(
	DTCTableDefinition *tabdef, int row_size,
	const std::vector<uint8_t> &field_map,
	std::vector<std::pair<int, bool> > &order_by_fields)
	: m_max_row_size(row_size),
	  m_comparator(OrderByComparator(tabdef, field_map, order_by_fields)),
	  m_raw_rows(m_comparator)
{
}

void RocksdbOrderByUnit::add_row(const OrderByUnitElement &ele)
{
	m_raw_rows.push(ele);

	if (m_raw_rows.size() > m_max_row_size)
		m_raw_rows.pop();

	return;
}

int RocksdbOrderByUnit::get_row(OrderByUnitElement &ele)
{
	if (m_raw_rows.empty())
		return 0;

	ele = m_raw_rows.top();
	m_raw_rows.pop();

	return 1;
}
