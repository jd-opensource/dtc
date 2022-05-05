/*
* timemanager.h
*
*  Created on: 2016.9.5
*  Author: qiulu
*/
#ifndef __TIMEMANAGER_H__
#define __TIMEMANAGER_H__

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
	TypeName(const TypeName&);               \
	void operator=(const TypeName&)

#include "list.h"
#include "timestamp.h"
#include <stdio.h>

class TimeManager {

public:
	static TimeManager& Instance()
	{
		static TimeManager instance;
		return instance;
	}
	void AdjustDay();
	//void InitDay();
	char *TimeStr();
	char *YdayTimeStr();
	char *TimeToStr();
	char *NextDayTimeStr();
	char *BeforeYesterDayTimeStr();
	time_t StrToTime(const char *sz_time);
private:

	TimeManager() :m_day(-1), m_year(-1), m_month(-1)
	{
		
	}
	~TimeManager() {};
	TimeManager(const TimeManager&);
	TimeManager& operator=(const TimeManager&);

private:
	
	int m_day;
	int m_year;
	int m_month;
	char m_sztime[100];
	char m_szndaytime[100];
	char m_szydaytime[100];
	char m_sz2ydaytime[100];

	
};

#endif
