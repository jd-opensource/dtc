#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>

#include "dtcapi.h"

void insert(DTC::Server server)
{
	int retCode = 0;

	DTC::InsertRequest insertReq(&server);
	DTC::Result stResult;

	insertReq.set_key(1);
	insertReq.Set("uid", 1);
	insertReq.Set("name", "norton");
	insertReq.Set("city", "shanghai");
	insertReq.Set("age", 18);
	insertReq.Set("sex", 1);

	retCode = insertReq.do_execute(stResult);
	if (retCode != 0)
		printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode,
		       stResult.get_error_message(), stResult.get_error_from());
	else
		printf("insert success!\n");
}

int main(int argc, char *argv[])
{
	/* 只要server不析构，后台会保持长连接 */
	DTC::Server server;

	/* 设置的dtc的ip和端口 */
	server.set_address("127.0.0.1", "20015");
	/* 设置网络超时时间,单次网络IO的超时,单位秒 */
	server.SetTimeout(5);
	/* 设置dtc的表名 */
	server.set_table_name("dtc_opensource");
	/* 声明key类型 */
	server.int_key();

	insert(server);

	return 0;
}
