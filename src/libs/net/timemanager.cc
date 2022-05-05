/*
* timemanager.cc
*
*  Created on: 2016.9.5
*  Author: qiulu
*/
#include "timemanager.h"




char* TimeManager::TimeStr()
{
	return m_sztime;
}

char* TimeManager::YdayTimeStr()
{
	return m_szydaytime;
}

char *TimeManager::BeforeYesterDayTimeStr()
{
	return m_sz2ydaytime;
}


char *TimeManager::NextDayTimeStr()
{
	return m_szndaytime;
}

time_t TimeManager::StrToTime(const char *sz_time)
{
	tm tm_;
	int year, month, day, hour, minute, second;
	sscanf(sz_time, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second);
	tm_.tm_year = year - 1900;
	tm_.tm_mon = month - 1;
	tm_.tm_mday = day;
	tm_.tm_hour = hour;
	tm_.tm_min = minute;
	tm_.tm_sec = second;
	tm_.tm_isdst = 0;
	time_t t_ = mktime(&tm_); 	
	return t_; 

}


char *TimeManager::TimeToStr()
{
	char *sz_time = new char[100];
	if (sz_time == NULL)  return NULL;
	time_t tt = time(NULL);
	struct tm *p = localtime(&tt);
	strftime(sz_time, 100 , "%Y-%m-%d %H:%M:%S", p);
	return sz_time;
}

void TimeManager::AdjustDay()
{
	time_t tt = time(NULL);
	tm* t = localtime(&tt);
	int y, m, d;
	if (t->tm_mday != m_day)
	{
	// update today 
		m_year = t->tm_year+1900;
		m_month = t->tm_mon+1;
		m_day = t->tm_mday;
		sprintf(m_sztime, "%04d%02d%02d", m_year, m_month, m_day);
	// update yesterday
		time_t t2 = tt - 86400;
		t = localtime(&t2);
		y = t->tm_year + 1900;
		m = t->tm_mon + 1;
		d = t->tm_mday;
		sprintf(m_szydaytime, "%04d%02d%02d", y, m, d);

		// update tomorrow
		t2 = tt + 86400;
		t = localtime(&t2);
		y = t->tm_year + 1900;
		m = t->tm_mon + 1;
		d = t->tm_mday;
		sprintf(m_szndaytime, "%04d%02d%02d", y, m, d);


		time_t t3 = t2 - 86400;
		t = localtime(&t3);
		y = t->tm_year + 1900;
		m = t->tm_mon + 1;
		d = t->tm_mday;
		sprintf(m_sz2ydaytime, "%04d%02d%02d", y, m, d);
		//return true;
	}
	//return false;
}






