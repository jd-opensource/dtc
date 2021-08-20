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

	DTC::Server stServer; // 只要server不析构，后台会保持长连接
	stServer.int_key(); // 声明key类型
	stServer.set_table_name("t_dtc_example");//设置dtc的表名，与table.conf中tablename应该一样
	stServer.set_address("192.168.214.62", "10009");//设置的dtc的ip和端口
	stServer.SetTimeout(5); // 设置网络超时时间
	stServer.set_accesskey("0000010284d9cfc2f395ce883a41d7ffc1bbcf4e"); // 设置访问码 AccessToken，在申请dtc实例的时候网站端会生成

	DTC::DeleteRequest deleteReq(&stServer);
	uid = atoi(argv[1]);
	deleteReq.set_key(uid);

	DTC::Result stResult;
	retCode = deleteReq.do_execute(stResult);
	printf("retCode:%d\n", retCode);
	if(retCode == 0)
	{
		printf("delete success!\n");
	}

	return 0;  
}  
