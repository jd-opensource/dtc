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

#ifndef __ROCKSDB_ORDER_BY_UNIT_H_
#define __ROCKSDB_ORDER_BY_UNIT_H_

#include "table/table_def.h"
#include <log/log.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

#include <string>
#include <vector>
#include <queue>

struct OrderByUnitElement {
	std::vector<std::string> m_rocks_keys;
	std::vector<std::string> m_rocks_values;

	const std::string &getValue(int fid) const
	{
		if (fid < m_rocks_keys.size())
			return m_rocks_keys[fid];
		return m_rocks_values[fid - m_rocks_keys.size()];
	}
};

class RocksdbOrderByUnit {
    private:
	class OrderByComparator {
	    private:
		DTCTableDefinition *table_define;
		std::vector<uint8_t> m_rock_field_indexes;
		std::vector<std::pair<int, bool /* asc is true */> >
			m_orderby_fields;

	    public:
		OrderByComparator(
			DTCTableDefinition *tab,
			const std::vector<uint8_t> &field_map,
			std::vector<std::pair<int, bool> > &order_by_fields)
			: table_define(tab), m_rock_field_indexes(field_map),
			  m_orderby_fields(order_by_fields)
		{
		}

		bool operator()(const OrderByUnitElement &lhs,
				const OrderByUnitElement &rhs) const
		{
			for (size_t idx = 0; idx < m_orderby_fields.size();
			     idx++) {
				int field_type = table_define->field_type(
					m_orderby_fields[idx].first);
				int rocks_fid = m_rock_field_indexes
					[m_orderby_fields[idx].first];
				const std::string &ls = lhs.getValue(rocks_fid);
				const std::string &rs = rhs.getValue(rocks_fid);
				switch (field_type) {
				case DField::Signed: {
					int64_t li =
						strtoll(ls.c_str(), NULL, 10);
					int64_t ri =
						strtoll(rs.c_str(), NULL, 10);
					if (li == ri)
						break;
					return m_orderby_fields[idx]
							       .second /*asc*/ ?
						       li < ri :
						       li > ri;
				}

				case DField::Unsigned: {
					uint64_t li =
						strtoull(ls.c_str(), NULL, 10);
					uint64_t ri =
						strtoull(rs.c_str(), NULL, 10);
					if (li == ri)
						break;
					return m_orderby_fields[idx]
							       .second /*asc*/ ?
						       li < ri :
						       li > ri;
				}

				case DField::Float: {
					float li = strtod(ls.c_str(), NULL);
					float ri = strtod(rs.c_str(), NULL);
					if (li == ri)
						break;
					return m_orderby_fields[idx]
							       .second /*asc*/ ?
						       li < ri :
						       li > ri;
				}

				case DField::String: {
					// case ignore
					int llen = ls.length();
					int rlen = rs.length();
					int ret = strncasecmp(
						ls.c_str(), rs.c_str(),
						llen < rlen ? llen : rlen);
					if (0 == ret && llen == rlen)
						break;
					return m_orderby_fields[idx]
							       .second /*asc*/ ?
						       (ret < 0 ||
							(ret == 0 &&
							 llen < rlen)) :
						       (ret > 0 ||
							(ret == 0 &&
							 llen > rlen));
				}
				case DField::Binary: {
					int llen = ls.length();
					int rlen = rs.length();
					int ret = strncmp(
						ls.c_str(), rs.c_str(),
						llen < rlen ? llen : rlen);
					if (0 == ret && llen == rlen)
						break;
					return m_orderby_fields[idx]
							       .second /*asc*/ ?
						       (ret < 0 ||
							(ret == 0 &&
							 llen < rlen)) :
						       (ret > 0 ||
							(ret == 0 &&
							 llen > rlen));
				}

				default:
					log4cplus_error(
						"unrecognized field type",
						m_orderby_fields[idx].first,
						field_type);
					return -1;
				}
			}
		};
	};

    private:
	int m_max_row_size;
	OrderByComparator m_comparator;
	std::priority_queue<OrderByUnitElement, std::vector<OrderByUnitElement>,
			    OrderByComparator>
		m_raw_rows;

    public:
	RocksdbOrderByUnit(DTCTableDefinition *tabdef, int row_size,
			   const std::vector<uint8_t> &field_map,
			   std::vector<std::pair<int, bool> > &order_by_fields);

	void add_row(const OrderByUnitElement &ele);
	int get_row(OrderByUnitElement &ele);

	/*
   * order in heap is opposite with the 'order by' semantic, wo need to reverse it
   * ig. 
   *   order by 'field' asc
   * element in heap will be: 7 5 3 1 -1, and the semantic of order by is: -1 1 3 5 7
   * */
	void reverse_rows()
	{
		// use deque to store rows can avoid this issuse, no need to implement
		return;
	}
};

#endif // __ROCKSDB_ORDER_BY_UNIT_H_
