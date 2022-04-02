/*
 * =====================================================================================
 *
 *       Filename:  registor.h
 *
 *    Description:  registor class definition.
 *
 *        Version:  1.0
 *        Created:  13/01/2021
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  chenyujie, chenyujie28@jd.com@jd.com
 *        Company:  JD.com, Inc.
 *
 * =====================================================================================
 */

#ifndef __HB_REGISTOR_H
#define __HB_REGISTOR_H
// local
#include "async_file.h"
// libs/api/cc_api/include
#include "dtcapi.h"
// common
#include "log/log.h"

class CRegistor {
public:
	CRegistor()
	: _master(0)
	, _controller()
	, _master_ctime(-1) {

	} ~CRegistor() {

	}

	int Init(DTC::Server * m, DTC::Server * s) {
		_master = m;
		return _controller.Init();
	}

	int Init() {return _controller.Init();}
	void SetMasterServer(DTC::Server * m) { _master = m; }
    int Regist();

	void ClearJournalID() { _controller.JournalId() = 0x0;}
	JournalID& JournalId(void) {
		return _controller.JournalId();
	}

	void SetSyncStatus(int iState) {
		_controller.SetDirty(iState);
	}

	int GetSyncStaus() { return _controller.GetDirty(); }
	/*
	 * 定期检查双方的共享内存，确保二者始终没有变化过。
	 *
	 * 如果slave上记录的内存创建时间和master共享内存
	 * 创建时间相同，证明二者的内存自创建以来，没有
	 * 被删除过，返回0， 否则返回1
	 */
	int CheckMemoryCreateTime() {
		int64_t v0;

		if (_master_ctime <= 0) {
			log4cplus_info("please invoke \"Regist\" function first");
			return 0;
		}

		v0 = QueryMemoryCreateTime(_master, 1);
		if (v0 > 0) {
			if (v0 != _master_ctime) {
				log4cplus_error("master memory changed");
				return -1;
			}
		}

		return 0;
	}
#ifdef UINT64FMT
# undef UINT64FMT
#endif
#if __WORDSIZE == 64
# define UINT64FMT "%lu"
#else
# define UINT64FMT "%llu"
#endif

	/* 
	 * 设置hbp状态为 "全量同步未完成" 
	 * 当hbp出现任何不可恢复的错误时，应该invoke这个接口
	 *
	 */
	// void SetHBPStatusDirty() {
	// 	_controller.SetDirty();
	// }

private:

	// inline int VerifyMemoryCreateTime(long long m, long long s) {
	// 	DTC::SvrAdminRequest rq(_slave);
	// 	rq.SetAdminCode(DTC::VerifyHBT);

	// 	rq.SetMasterHBTimestamp(m);
	// 	rq.SetSlaveHBTimestamp(s);

	// 	DTC::Result rs;
	// 	rq.Execute(rs);

	// 	if (rs.ResultCode() == -DTC::EC_ERR_SYNC_STAGE) {
	// 		return -1;
	// 	}

	// 	return 0;
	// }

	inline int64_t QueryMemoryCreateTime(DTC::Server * svr, int master) {
		DTC::SvrAdminRequest rq(svr);
		rq.SetAdminCode(DTC::GetHBTime);

		DTC::Result rs;
		rq.Execute(rs);

		if (rs.ResultCode() != 0)
			return -1;

		if(master)
			return rs.MasterHBTimestamp();
		else
			return rs.SlaveHBTimestamp();
	}

private:
	DTC::Server * _master;
	CAsyncFileController _controller;
	/* master memory create time */
	int64_t _master_ctime;
};

#endif
