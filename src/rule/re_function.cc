#include "re_function.h"
#include <string.h>
#include "log.h"

time_t convert_time_interval(time_t val, int type)
{
    if(type == kDatetimeSecond)
        return val;
    else if(type == kDatetimeMinute)
        return val * 60;
    else if(type == kDatetimeHour)
        return val * 60 * 60;
    else if(type == kDatetimeDay)
        return val * 60 * 60 * 24;
}

std::string fun_date_sub(std::vector<Expr*>* elist)
{
    char timestring[20] = {0};

    if(elist->size() != 2)
        return "";

    if((*elist)[1]->datetimeField == kDatetimeNone)
        return "";
    
    if((*elist)[0]->type == kExprLiteralString)
    {
        return "";
    }
    else if((*elist)[0]->type == kExprFunctionRef)
    {
        if(strcasecmp((*elist)[0]->name, "CURDATE") == 0)
        {
            int interval = (*elist)[1]->ival;
            time_t now;
            time(&now);

            if((*elist)[1]->datetimeField == kDatetimeMonth)
            {
                struct tm *p = localtime(&now);
                p->tm_year -= (interval / 12);
                int mod = interval % 12;
                if(mod > p->tm_mon)
                {
                    p->tm_year--;
                    p->tm_mon = 11 - p->tm_mon;
                }
                else
                    p->tm_mon -= mod;
        
                strftime(timestring, sizeof(timestring), "%Y-%m-%d", p);
                log4cplus_debug("new local: %s", timestring);
            }
            else if((*elist)[1]->datetimeField == kDatetimeYear)
            {
                struct tm *p = localtime(&now);
                p->tm_year -= interval;
                strftime(timestring, sizeof(timestring), "%Y-%m-%d", p);
                log4cplus_debug("new local: %s", timestring);
            }
            else if((*elist)[1]->datetimeField == kDatetimeDay)
            {
                tm* tm_now = NULL;
                now -= convert_time_interval(interval, (*elist)[1]->datetimeField);
                tm_now = localtime(&now);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d", tm_now);
                log4cplus_debug("new local: %s", timestring);
            }
            else
            {
                return "";
            }
        }
        else if(strcasecmp((*elist)[0]->name, "NOW") == 0)
        {
            int interval = (*elist)[1]->ival;
            time_t now;
            time(&now);

            if((*elist)[1]->datetimeField == kDatetimeMonth)
            {
                struct tm *p = localtime(&now);
                p->tm_year -= (interval / 12);
                int mod = interval % 12;
                if(mod > p->tm_mon)
                {
                    p->tm_year--;
                    p->tm_mon = 11 - p->tm_mon;
                }
                else
                    p->tm_mon -= mod;
        
                log4cplus_debug("new localtime: %d-%d-%d %d:%d:%d, value: %d, type: %d", p->tm_year, p->tm_mon + 1, p->tm_mday, p->tm_hour, p->tm_min, p->tm_sec, interval, (*elist)[1]->datetimeField);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d %H:%M:%S", p);
            }
            else if((*elist)[1]->datetimeField == kDatetimeYear)
            {
                struct tm *p = localtime(&now);
                p->tm_year -= interval;
                log4cplus_debug("new localtime: %d-%d-%d %d:%d:%d, value: %d, type: %d", p->tm_year, p->tm_mon + 1, p->tm_mday, p->tm_hour, p->tm_min, p->tm_sec, interval, (*elist)[1]->datetimeField);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d %H:%M:%S", p);
            }
            else
            {
                tm* tm_now = NULL;
                now -= convert_time_interval(interval, (*elist)[1]->datetimeField);
                tm_now = localtime(&now);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d %H:%M:%S", tm_now);
            }
        }
    }

    return timestring;
}

std::string fun_date_add(std::vector<Expr*>* elist)
{
    char timestring[20] = {0};

    if(elist->size() != 2)
        return "";

    if((*elist)[1]->datetimeField == kDatetimeNone)
        return "";
    
    if((*elist)[0]->type == kExprLiteralString)
    {
        return "";
    }
    else if((*elist)[0]->type == kExprFunctionRef)
    {
        if(strcasecmp((*elist)[0]->name, "CURDATE") == 0)
        {
            int interval = (*elist)[1]->ival;
            time_t now;
            time(&now);

            if((*elist)[1]->datetimeField == kDatetimeMonth)
            {
                struct tm *p = localtime(&now);
                p->tm_year += (interval / 12);
                int mod = interval % 12;
                if(mod + p->tm_mon > 11)
                {
                    p->tm_year++;
                    p->tm_mon = 11 - p->tm_mon;
                }
                else
                    p->tm_mon += mod;
        
                strftime(timestring, sizeof(timestring), "%Y-%m-%d", p);
                log4cplus_debug("new local: %s", timestring);
            }
            else if((*elist)[1]->datetimeField == kDatetimeYear)
            {
                struct tm *p = localtime(&now);
                p->tm_year += interval;
                strftime(timestring, sizeof(timestring), "%Y-%m-%d", p);
                log4cplus_debug("new local: %s", timestring);
            }
            else if((*elist)[1]->datetimeField == kDatetimeDay)
            {
                tm* tm_now = NULL;
                now += convert_time_interval(interval, (*elist)[1]->datetimeField);
                tm_now = localtime(&now);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d", tm_now);
                log4cplus_debug("new local: %s", timestring);
            }
            else
            {
                return "";
            }
        }
        else if(strcasecmp((*elist)[0]->name, "NOW") == 0)
        {
            int interval = (*elist)[1]->ival;
            time_t now;
            time(&now);

            if((*elist)[1]->datetimeField == kDatetimeMonth)
            {
                struct tm *p = localtime(&now);
                p->tm_year += (interval / 12);
                int mod = interval % 12;
                if(mod + p->tm_mon > 11)
                {
                    p->tm_year++;
                    p->tm_mon = 11 - p->tm_mon;
                }
                else
                    p->tm_mon += mod;
        
                log4cplus_debug("new localtime: %d-%d-%d %d:%d:%d, value: %d, type: %d", p->tm_year, p->tm_mon + 1, p->tm_mday, p->tm_hour, p->tm_min, p->tm_sec, interval, (*elist)[1]->datetimeField);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d %H:%M:%S", p);
            }
            else if((*elist)[1]->datetimeField == kDatetimeYear)
            {
                struct tm *p = localtime(&now);
                p->tm_year += interval;
                log4cplus_debug("new localtime: %d-%d-%d %d:%d:%d, value: %d, type: %d", p->tm_year, p->tm_mon + 1, p->tm_mday, p->tm_hour, p->tm_min, p->tm_sec, interval, (*elist)[1]->datetimeField);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d %H:%M:%S", p);
            }
            else
            {
                tm* tm_now = NULL;
                now += convert_time_interval(interval, (*elist)[1]->datetimeField);
                tm_now = localtime(&now);
                strftime(timestring, sizeof(timestring), "%Y-%m-%d %H:%M:%S", tm_now);
            }

            return timestring;
        }
    }

    return "";
}