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
#include <unistd.h>
#include <errno.h>
#include <endian.h>
#include <byteswap.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>

#include "task_base.h"
#include "../decode/decode.h"
#include "protocol.h"
#include "dtc_error_code.h"
#include "../log/log.h"

int DtcJob::append_result(ResultSet *rs)
{
	if (rs == NULL) {
		set_total_rows(0);
		return 0;
	}
	rs->rewind();
	if (all_rows() && (count_only() || !in_range(rs->total_rows(), 0))) {
		set_total_rows(rs->total_rows());
	} else {
		for (int i = 0; i < rs->total_rows(); i++) {
			const RowValue *r = rs->fetch_row();
			if (r == NULL)
				return rs->error_num();
			if (compare_row(*r)) {
				log4cplus_debug("append_row flag");
				int rv = resultWriter->append_row(*r);
				if (rv < 0)
					return rv;
				if (all_rows() && result_full()) {
					set_total_rows(rs->total_rows());
					break;
				}
			}
		}
	}
	return 0;
}

/* all use prepareResult interface */
int DtcJob::pass_all_result(ResultSet *rs)
{
	prepare_result(0, 0);

	rs->rewind();
	if (count_only()) {
		set_total_rows(rs->total_rows());
	} else {
		for (int i = 0; i < rs->total_rows(); i++) {
			int rv;
			const RowValue *r = rs->fetch_row();
			if (r == NULL) {
				rv = rs->error_num();
				set_error(rv, "fetch_row()", NULL);
				return rv;
			}
			log4cplus_debug("append_row flag");
			rv = resultWriter->append_row(*r);
			if (rv < 0) {
				set_error(rv, "append_row()", NULL);
				return rv;
			}
		}
	}
	resultWriter->set_total_rows((unsigned int)(resultInfo.total_rows()));

	return 0;
}

int DtcJob::merge_result(const DtcJob &job)
{
	/*首先根据子包击中情况统计父包的名种情况*/
	uint32_t uChildTaskHitFlag = job.resultInfo.hit_flag();
	if (HIT_SUCCESS == uChildTaskHitFlag) {
		resultInfo.incr_tech_hit_num();
		ResultPacket *pResultPacket =
			job.result_code() >= 0 ? job.get_result_packet() : NULL;
		if (pResultPacket &&
		    (pResultPacket->numRows || pResultPacket->totalRows)) {
			resultInfo.incr_bussiness_hit_num();
		}
	}

	int ret = job.resultInfo.affected_rows();

	if (ret > 0) {
		resultInfo.set_affected_rows(ret + resultInfo.affected_rows());
	}

	if (job.resultWriter == NULL)
		return 0;

	if (resultWriter == NULL) {
		if ((ret = prepare_result_no_limit()) != 0) {
			return ret;
		}
	}
	return resultWriter->merge_no_limit(job.resultWriter);
}

/* the only entry to create resultWriter */
int DtcJob::prepare_result(int st, int ct)
{
	int err;

	if (resultWriterReseted && resultWriter)
		return 0;

	if (resultWriter) {
		err = resultWriter->Set(fieldList, st, ct);
		if (err < 0) {
			set_error(-EC_TASKPOOL, "ResultPacket()", NULL);
			return err;
		}
	} else {
		try {
			resultWriter = new ResultPacket(fieldList, st, ct);
		} catch (int err) {
			set_error(err, "ResultPacket()", NULL);
			return err;
		}
	}

	resultWriterReseted = 1;

	return 0;
}
