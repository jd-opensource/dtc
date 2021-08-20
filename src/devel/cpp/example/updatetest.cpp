#include<stdio.h>
#include<stdlib.h>
#include<string>
#include<stdint.h>
#include<unistd.h>

#include "dtcapi.h"

int main(int argc,char* argv[])
{
	int retCode = 0;
	unsigned int uid;
	unsigned int age;
	std::string name;
	std::string city;
	std::string descr;

	DTC::Server stServer; // 只要server不析构，后台会保持长连接
	stServer.int_key(); // 声明key类型
	stServer.set_table_name("t_dtc_example");//设置dtc的表名，与table.conf中tablename应该一样
	stServer.set_address("192.168.214.62", "10009");//设置的dtc的ip和端口
	stServer.SetTimeout(5); // 设置网络超时时间
	stServer.set_accesskey("0000010284d9cfc2f395ce883a41d7ffc1bbcf4e"); // 设置访问码 AccessToken，在申请dtc实例的时候网站端会生成
	
	uid = atoi(argv[1]);
	name = std::string(argv[2]);
	city = std::string(argv[3]);
	descr = std::string(argv[4]);
	age = atoi(argv[5]);
	DTC::UpdateRequest UpdateReq(&stServer);
	retCode = UpdateReq.set_key(uid);
	if(retCode != 0)
	{
		printf("update-req set key error: %d", retCode);
		fflush(stdout);
		return(-1);
	}
	retCode = UpdateReq.Set("name", name.c_str());
	retCode = UpdateReq.Set("city", city.c_str());
	retCode = UpdateReq.Set("descr", descr.c_str());
	retCode = UpdateReq.Set("age", age);
	if(retCode != 0)
	{
		printf("update-req set field error: %d", retCode);
		fflush(stdout);
		return(-1);
	}

	// do_execute & get result
	DTC::Result stResult;
	retCode = UpdateReq.do_execute(stResult);
	printf("retCode:%d\n", retCode);
	if(retCode == 0)
	{
		DTC::GetRequest getReq(&stServer);
		getReq.set_key(uid);
		if(retCode == 0)
			retCode = getReq.need("uid");//设置需要select的字段，注意第一个key字段不能在这里出现
		if(retCode == 0)
			retCode = getReq.need("name");
		if(retCode == 0)
			retCode = getReq.need("city");
		if(retCode == 0)
			retCode = getReq.need("descr");
		if(retCode == 0)
			retCode = getReq.need("age");
		if(retCode != 0)
		{
			printf("get-req set key or need error: %d", retCode);
			fflush(stdout);
			return(-1);
		}

		// do_execute & get result
		retCode = getReq.do_execute(stResult);
		retCode = stResult.fetch_row();//开始获取数据
		printf("uid:%d\n", stResult.int_value("uid"));
	    printf("name: %s\n", stResult.string_value("name"));//输出binary类型的数据
		printf("city: %s\n", stResult.string_value("city"));
		printf("descr: %s\n", stResult.binary_value("descr"));
	    printf("age: %d\n", stResult.int_value("age"));//输出int类型的数据
	}
	return 0;
}  
