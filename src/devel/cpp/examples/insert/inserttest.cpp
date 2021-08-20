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
	unsigned int lv = 0;
	std::string name;
	std::string city;
	std::string descr;
	unsigned int salary;

	unsigned int uid1;
	unsigned int age1;
	std::string name1;
	std::string city1;
	std::string descr1;
	unsigned int salary1;

	lv = atoi(argv[1]);

	DTC::Server stServer; // 只要server不析构，后台会保持长连接
	stServer.int_key(); // 声明key类型
	stServer.set_table_name("t_dtc_example");//设置dtc的表名，与table.conf中tablename应该一样
	stServer.set_address("192.168.214.62", "10901");//设置的dtc的ip和端口
	stServer.SetTimeout(5); // 设置网络超时时间
	stServer.set_accesskey("0000090184d9cfc2f395ce883a41d7ffc1bbcf4e"); // 设置访问码 AccessToken，在申请dtc实例的时候网站端会生成

	DTC::InsertRequest insertReq(&stServer);
	//retCode = insertReq.set_key(key);

	uid = atoi(argv[2]);
	name = std::string(argv[3]);
	city = std::string(argv[4]);
	descr = std::string(argv[5]);
	age = atoi(argv[6]);
	salary = atoi(argv[7]);

	insertReq.set_key(uid);
	//insertReq.Set("key", 100003);
	insertReq.Set("uid", uid);
	insertReq.Set("name", name.c_str());
	insertReq.Set("city", city.c_str());
	insertReq.Set("descr", descr.c_str());
	insertReq.Set("age", age);
	insertReq.Set("salary", salary);

	DTC::Result stResult;
	retCode = insertReq.do_execute(stResult);
	printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode, stResult.get_error_message(), stResult.get_error_from());
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
		if(retCode == 0)
			retCode = getReq.need("salary");
		if(retCode != 0)
		{
			printf("get-req set key or need error: %d", retCode);
			fflush(stdout);
			return(-1);
		}

		// do_execute & get result
		stResult.Reset();
		retCode = getReq.do_execute(stResult);
		printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode, stResult.get_error_message(), stResult.get_error_from());
		retCode = stResult.fetch_row();//开始获取数据
		printf("uid:%lu\n", stResult.int_value("uid"));
	    printf("name: %s\n", stResult.string_value("name"));//输出binary类型的数据
		printf("city: %s\n", stResult.string_value("city"));
		printf("descr: %s\n", stResult.binary_value("descr"));
	    printf("age: %lu\n", stResult.int_value("age"));//输出int类型的数据
	    printf("salary: %lu\n", stResult.int_value("salary"));//输出int类型的数据
	}

	printf("-------------first request end -----------------\n");
	printf("-------------second request begin---------------\n");

	DTC::Server stServer1; // 只要server不析构，后台会保持长连接
	stServer1.int_key(); // 声明key类型
	stServer1.set_table_name("tp1");//设置dtc的表名，与table.conf中tablename应该一样
	stServer1.set_address("192.168.214.62", "10201");//设置的dtc的ip和端口
	stServer1.SetTimeout(5); // 设置网络超时时间
	stServer1.set_accesskey("0000020184d9cfc2f395ce883a41d7ffc1bbcf4e"); // 设置访问码 AccessToken，在申请dtc实例的时候网站端会生成

	DTC::InsertRequest insertReq1(&stServer1);
	//retCode = insertReq.set_key(key);

	uid1 = atoi(argv[8]);
	name1 = std::string(argv[9]);
	city1 = std::string(argv[10]);
	descr1 = std::string(argv[11]);
	age1 = atoi(argv[12]);
	salary1 = atoi(argv[13]);

	insertReq1.set_key(uid1);
	//insertReq.Set("key", 100003);
	insertReq1.Set("uid", uid1);
	insertReq1.Set("name", name1.c_str());
	insertReq1.Set("city", city1.c_str());
	insertReq1.Set("descr", descr1.c_str());
	insertReq1.Set("age", age1);
	insertReq1.Set("salary", salary1);

	DTC::Result stResult1;
	retCode = insertReq1.do_execute(stResult1);
	printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode, stResult1.get_error_message(), stResult1.get_error_from());
	if(retCode == 0)
	{
		DTC::GetRequest getReq1(&stServer1);
		getReq1.set_key(uid);
		if(retCode == 0)
			retCode = getReq1.need("uid");//设置需要select的字段，注意第一个key字段不能在这里出现
		if(retCode == 0)
			retCode = getReq1.need("name");
		if(retCode == 0)
			retCode = getReq1.need("city");
		if(retCode == 0)
			retCode = getReq1.need("descr");
		if(retCode == 0)
			retCode = getReq1.need("age");
		if(retCode == 0)
			retCode = getReq1.need("salary");
		if(retCode != 0)
		{
			printf("get-req set key or need error: %d", retCode);
			fflush(stdout);
			return(-1);
		}

		// do_execute & get result
		stResult1.Reset();
		retCode = getReq1.do_execute(stResult1);
		printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode, stResult1.get_error_message(), stResult1.get_error_from());
		retCode = stResult1.fetch_row();//开始获取数据
		printf("uid:%lu\n", stResult1.int_value("uid"));
	    printf("name: %s\n", stResult1.string_value("name"));//输出binary类型的数据
		printf("city: %s\n", stResult1.string_value("city"));
		printf("descr: %s\n", stResult1.binary_value("descr"));
	    printf("age: %lu\n", stResult1.int_value("age"));//输出int类型的数据
	    printf("salary: %lu\n", stResult1.int_value("salary"));//输出int类型的数据
	}

	return 0;  
}  
