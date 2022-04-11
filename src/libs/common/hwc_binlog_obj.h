#ifndef HWC_BINLOG_OBJ_H_
#define HWC_BINLOG_OBJ_H_

#include <string.h>
#include "mem_check.h"

#include "log/log.h"

#define INT_SIZE        sizeof(int)

struct HwcBinlogCont
{
    int i_sql_len;              // sql statement length
    char* p_sql;                // sql statement
    int i_check_flag;           // check flag, 0:pass ,1:checking
    int i_raw_nums;             // raw nums, begin at 1
    int i_raw_len;              // raw value length 
    char* p_raw_val;            // raw value

    HwcBinlogCont()
        : i_sql_len(0)
        , p_sql(NULL)
        , i_check_flag(0)
        , i_raw_nums(0)
        , i_raw_len(0)
        , p_raw_val(NULL)
    { }

    ~HwcBinlogCont()
    { }

    void Clear() {
        FREE_IF(p_sql);
        FREE_IF(p_raw_val);
    }
    
    bool ParseFromString(
        const char* c_buf,
        int i_len)
    {
        log4cplus_info("total len:%d , len:%d" , total_length() , i_len);
        if (total_length() > i_len) {
            return false;
        }

        this->i_sql_len = parse_int(c_buf);
        this->p_sql = parse_str(c_buf , i_sql_len);
        this->i_check_flag = parse_int(c_buf);
        if (i_check_flag) {
            this->i_raw_nums = parse_int(c_buf);
            this->i_raw_len = parse_int(c_buf);
            this->p_raw_val = parse_str(c_buf , i_raw_len);
        }
        return true;
    };

    bool SerializeToString(char* p_buf)
    {
        log4cplus_info("line:%d " , __LINE__);
        if (!p_buf) { return false; }
        
        append_int_to_buf(p_buf , this->i_sql_len);
        append_char_to_buf(p_buf , this->p_sql , this->i_sql_len);
        append_int_to_buf(p_buf , this->i_check_flag);
        log4cplus_info("line:%d " , __LINE__);
        if (i_check_flag) {
            append_int_to_buf(p_buf , this->i_raw_nums);
            append_int_to_buf(p_buf , this->i_raw_len);
            append_char_to_buf(p_buf , this->p_raw_val , this->i_raw_len);
        }
        log4cplus_info("line:%d " , __LINE__);
        return true;
    };

    int total_length() {
        int i_total_len = 2*INT_SIZE + i_sql_len;
        if (i_check_flag) {
            i_total_len += (2*INT_SIZE + i_raw_len);
        }
        return i_total_len;
    }

private:
    void append_int_to_buf(
        char*& c_buf ,
        const int& i_val)
    {
        memcpy((void*)(c_buf) , (void*)&i_val , INT_SIZE);
        c_buf += INT_SIZE;
    }

    void append_char_to_buf(
            char*& c_buf ,
            const char* src_buf,
            const int& i_len) 
    {
        memcpy((void*)(c_buf) , (void*)src_buf , i_len);
        c_buf += i_len;
    }

    int parse_int(const char*& c_buf) {
        int i_value = *(int*)c_buf;
        c_buf += INT_SIZE;
        return i_value;
    }

    char* parse_str(const char*& c_buf , int i_len) {
        char* p_value = (char*)MALLOC(i_len + 1);
        memcpy((void*)p_value , (void*)c_buf , i_len);
        c_buf += i_len;
        p_value[i_len] = '\0';
        return p_value;
    }
};

#endif