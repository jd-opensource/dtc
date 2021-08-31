#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>

#include "dtcapi.h"

void select(DTC::Server server) {
  /* 直接构造指定操作的对象：查询操作 */
  DTC::GetRequest get_request(&server);
  int uid;
  /* 执行的结果:包含错误信息以及返回的row数据 */
  DTC::Result result;
  int iRet;

  get_request.set_key(1);
  uid = 1;

  /**
   * Signed = 1,	     Signed Integer
   * Unsigned = 2,     Unsigned Integer
   * FloatPoint = 3    FloatPoint
   * String = 4,	     String, case insensitive, null ended
   * Binary = 5,	     opaque binary data
   */
  printf("FieldType uid: %d\n", server.get_field_type("uid"));
  printf("FieldType name: %d\n", server.get_field_type("name"));
  printf("FieldType city: %d\n", server.get_field_type("city"));
  printf("FieldType age: %d\n", server.get_field_type("age"));
  printf("FieldType sex: %d\n", server.get_field_type("sex"));
  printf("==========================================================\n");

  /* 设置需要select的字段 */
  if (iRet == 0)
    iRet = get_request.need("uid");
  if (iRet == 0)
    iRet = get_request.need("name");
  if (iRet == 0)
    iRet = get_request.need("city");
  if (iRet == 0)
    iRet = get_request.need("age");
  if (iRet == 0)
    iRet = get_request.need("sex");
  if (iRet != 0) {
    printf("get-req need error: %d", iRet);
    fflush(stdout);
  }

  /* 提交请求，执行结果存在Result对象里 */
  iRet = get_request.do_execute(result);
  /* 如果出错,则输出错误码、错误阶段，错误信息，stResult.get_error_from(),
   * result.get_error_message()
   * 这两个错误信息很重要，一定要打印出来，方便定位问题 */
  if (iRet != 0) {
    printf("uin[%u] dtc execute get error: %d, error_from:%s, msg:%s\n",
           uid,                       // 出错的key是多少
           iRet,                      // 错误码为多少
           result.get_error_from(),   // 返回错误阶段
           result.get_error_message() // 返回错误信息
    );
    fflush(stdout);
  }
  /* 数据不存在 */
  if (result.get_num_row_size() <= 0)
    printf("uin[%u] data not exist.\n", uid);

  /* 读取结果的Key值 */
  printf("result key: %lld\n", result.int_key());

  /* 输出结果的行数 */
  printf("NumRows:%d\n", result.get_num_row_size());

  /* 输出结果 */
  for (int i = 0; i <= result.get_num_row_size(); ++i) {
    /* 读取一行数据 */
    iRet = result.fetch_row();
    if (iRet < 0) {
      printf("uid[%lu] dtc fetch row error: %d\n", uid, iRet);
      fflush(stdout);
    }
    /* 如果一切正确，则可以输出数据了 */
    /* 输出int类型的数据 */
    printf("uid: %lld\n", result.int_value("uid"));
    /* 输出binary类型的数据 */
    printf("name: %s\n", result.binary_value("name"));
    /* 输出string类型的数据 */
    printf("city: %s\n", result.string_value("city"));
    printf("age:%lld\n", result.int_value("age"));
    printf("sex:%lld\n", result.int_value("sex"));
  }
}

int main(int argc, char *argv[]) {
  /* 只要server不析构，后台会保持长连接 */
  DTC::Server server;
  /* 设置的dtc的ip和端口 */
  server.set_address("127.0.0.1", "20015");
  /* 设置网络超时时间,单次网络IO的超时,单位秒 */
  server.SetTimeout(5);
  /* 设置访问码 AccessToken，在申请dtc实例的时候网站端会生成 */
  // server.set_accesskey("000022907e64e117fa92f892a85307782a68afc6");
  /* 设置dtc的表名 */
  server.set_table_name("dtc_opensource");
  /* 声明key类型 */
  server.int_key();

  select(server);

  return 0;
}
