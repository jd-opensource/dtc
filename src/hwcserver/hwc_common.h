/*
 * =====================================================================================
 *
 *       Filename:  sync_common.h
 *
 *    Description:  sync_common class definition.
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

#ifndef HWC_COMMON_H_
#define HWC_COMMON_H_

#include <stdlib.h>
#include <map>

class HwcStateBase;

#define LOG_KEY_WORD  "[HWCState]:"
#define FOR_DEBUG     0

enum HWC_STATE_ENUM
{
    E_HWC_STATE_INIT,
    E_HWC_STATE_REGISTER,
    E_HWC_STATE_BINLOG_SYNC,
    E_HWC_STATE_FAULT
};

typedef std::map<int , HwcStateBase*> HwcStateMap;
typedef HwcStateMap::iterator HwcStateMapIter;

#endif