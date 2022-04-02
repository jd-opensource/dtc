/*
 * =====================================================================================
 *
 *       Filename:  system_state.h
 *
 *    Description:  system_state class definition.
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

#ifndef SYSTEM_STATE_H_
#define SYSTEM_STATE_H_
// common
#include "algorithm/non_copyable.h"
#include "algorithm/singleton.h"
#include "journal_id.h"

class DTCTableDefinition;

class SystemState : private noncopyable
{
public:
    SystemState() 
    : p_dtc_table_def_(NULL)
    , o_journal_id_()
    { };
    virtual ~SystemState() { };

public:
    static SystemState* Instance()
    {
        return Singleton<SystemState>::instance();
    };

    static void Destroy()
    {
        Singleton<SystemState>::destory();
    };

public:
    void SetDtcTabDef(const DTCTableDefinition* p_dtc_table_def) {
        p_dtc_table_def_ = p_dtc_table_def;
    };

    const DTCTableDefinition* GetDtcTabDef() {
        return p_dtc_table_def_;
    };

    void SetJournalID(const JournalID& o_jour_id) {
        o_journal_id_ = o_jour_id;
    }

    const JournalID& GetJournalID() {
        return o_journal_id_;
    }
    
private:
    DTCTableDefinition* p_dtc_table_def_;
    JournalID o_journal_id_;
};

#endif
