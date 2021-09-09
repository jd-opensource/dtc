#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>

#include "dtcapi.h"
#include "gtest/gtest.h"


class DTCClientAPITest: public testing::Test {
 protected: 
  void SetUp() override {
      
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
  }
  /* 只要server不析构，后台会保持长连接 */
  DTC::Server server;
};

int compare(int uid,DTC::Server server,DTC::Result &result,bool limit_switch = false)
{
  int retcode;
  DTC::GetRequest get_request(&server);
  get_request.set_key(uid);
  if(limit_switch == true)
     get_request.limit(2, 2);
  get_request.need("uid");
  get_request.need("name");
  get_request.need("city");
  get_request.need("age");
  get_request.need("sex");
  retcode = get_request.do_execute(result);
  return retcode;
}

TEST_F(DTCClientAPITest, INSERT_CORRENT_DATA) 
{
  DTC::InsertRequest insertReq(&server);
  DTC::Result result;
  insertReq.set_key(1);
  insertReq.Set("uid", 1);
  insertReq.Set("name", "汪汪");
  insertReq.Set("city", "上海");
  insertReq.Set("age", 3);
  insertReq.Set("sex", 1);
  EXPECT_EQ(0,insertReq.do_execute(result));
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    if(i == result.get_num_row_size())
    {
      EXPECT_EQ(1,result.int_value("uid"));
      EXPECT_STREQ("汪汪",result.binary_value("name"));
      EXPECT_STREQ("上海",result.string_value("city"));
      EXPECT_EQ(3,result.int_value("age"));
      EXPECT_EQ(1,result.int_value("sex"));
    }  
  }
}

TEST_F(DTCClientAPITest, INSERT_DELETION_FIELD) {

  DTC::InsertRequest insertReq(&server);
  DTC::Result result;
  insertReq.set_key(1);
  insertReq.Set("uid", 1);
  insertReq.Set("name", "喵喵");
  EXPECT_EQ(0,insertReq.do_execute(result));
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    if(i == result.get_num_row_size())
    {
      EXPECT_EQ(1,result.int_value("uid"));
      EXPECT_STREQ("喵喵",result.binary_value("name"));
      EXPECT_STREQ("",result.string_value("city"));
      EXPECT_EQ(0,result.int_value("age"));
      EXPECT_EQ(0,result.int_value("sex"));
    }  
  }
}

TEST_F(DTCClientAPITest, INSERT_DELETION_KEY) {

  DTC::InsertRequest insertReq(&server);
  DTC::Result result;
  insertReq.Set("city", "杭州");
  insertReq.Set("age", 9);
  EXPECT_EQ(-2032,insertReq.do_execute(result));
}

TEST_F(DTCClientAPITest, INSERT_BAD_TYPE) {

  DTC::InsertRequest insertReq(&server);
  DTC::Result result;
  insertReq.set_key("jd");
  EXPECT_EQ(-2024,insertReq.do_execute(result));
}

TEST_F(DTCClientAPITest, INSERT_BAD_ORDER) {

  DTC::InsertRequest insertReq(&server);
  DTC::Result result;
  insertReq.set_key(1);
  insertReq.Set("age", 2);
  insertReq.Set("name", "呱呱");
  insertReq.Set("uid", 1);
  insertReq.Set("city", "上海");
  insertReq.Set("sex", 0);
  EXPECT_EQ(0,insertReq.do_execute(result));
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    if(i == result.get_num_row_size())
    {
      EXPECT_EQ(1,result.int_value("uid"));
      EXPECT_STREQ("呱呱",result.binary_value("name"));
      EXPECT_STREQ("上海",result.string_value("city"));
      EXPECT_EQ(2,result.int_value("age"));
      EXPECT_EQ(0,result.int_value("sex"));
    }
  }
}

TEST_F(DTCClientAPITest, INSERT_SAME) {

  DTC::InsertRequest insertReq(&server);
  DTC::Result result;
  insertReq.set_key(1);
  insertReq.Set("uid", 1);
  insertReq.Set("name", "汪汪");
  insertReq.Set("city", "上海");
  insertReq.Set("age", 3);
  insertReq.Set("sex", 1);
  EXPECT_EQ(0,insertReq.do_execute(result));
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    if(i == result.get_num_row_size())
    {
      EXPECT_EQ(1,result.int_value("uid"));
      EXPECT_STREQ("汪汪",result.binary_value("name"));
      EXPECT_STREQ("上海",result.string_value("city"));
      EXPECT_EQ(3,result.int_value("age"));
      EXPECT_EQ(1,result.int_value("sex"));
    }  
  }
}

TEST_F(DTCClientAPITest, INSERT_ONCE_MULTIPLE) {

  DTC::InsertRequest insertReq(&server);
  DTC::Result result;
  insertReq.set_key(1);
  insertReq.Set("uid", 1);
  insertReq.Set("name", "咩咩");
  insertReq.Set("city", "西安");
  insertReq.Set("age", 7);
  insertReq.Set("sex", 1);
  EXPECT_EQ(0,insertReq.do_execute(result));
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    if(i == result.get_num_row_size())
    {
      EXPECT_EQ(1,result.int_value("uid"));
      EXPECT_STREQ("咩咩",result.binary_value("name"));
      EXPECT_STREQ("西安",result.string_value("city"));
      EXPECT_EQ(7,result.int_value("age"));
      EXPECT_EQ(1,result.int_value("sex"));
    }
  }

  insertReq.Reset();
  insertReq.set_key(2);
  insertReq.Set("uid", 2);
  insertReq.Set("name", "布谷");
  insertReq.Set("city", "福建");
  insertReq.Set("age", 11);
  insertReq.Set("sex", 0);
  EXPECT_EQ(0,insertReq.do_execute(result));
  EXPECT_EQ(0,compare(2,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    if(i == result.get_num_row_size())
    {
      EXPECT_EQ(2,result.int_value("uid"));
      EXPECT_STREQ("布谷",result.binary_value("name"));
      EXPECT_STREQ("福建",result.string_value("city"));
      EXPECT_EQ(11,result.int_value("age"));
      EXPECT_EQ(0,result.int_value("sex"));
    }
  }
}

  
  
typedef struct node{
  int uid;
	char name[10];
	char city[10];
	int age;
  int sex;
}Data;

Data data[5] = {
  {1,"汪汪","上海",3,1},
  {1,"喵喵","",0,0},
  {1,"呱呱","上海",2,0},
  {1,"汪汪","上海",3,1},
  {1,"咩咩","西安",7,1}
};


TEST_F(DTCClientAPITest, SELECT_CORRENT_KEY)
{
  DTC::Result result;
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 0; i < result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    EXPECT_EQ(data[i].uid,result.int_value("uid"));
    EXPECT_STREQ(data[i].name,result.binary_value("name"));
    EXPECT_STREQ(data[i].city,result.string_value("city"));
    EXPECT_EQ(data[i].age,result.int_value("age"));
    EXPECT_EQ(data[i].sex,result.int_value("sex"));
  }
}

TEST_F(DTCClientAPITest, SELECT_BAD_KEY)
{
  DTC::GetRequest get_request(&server);
  DTC::Result result;
  get_request.set_key(-1);
  EXPECT_EQ(0,get_request.do_execute(result));
}

TEST_F(DTCClientAPITest, SELECT_BAD_TYPE_KEY)
{
  DTC::GetRequest get_request(&server);
  DTC::Result result;
  get_request.set_key("jd");
  EXPECT_EQ(-2024,get_request.do_execute(result));
}

TEST_F(DTCClientAPITest, SELECT_NO_KEY_FIELD)
{
  DTC::GetRequest get_request(&server);
  DTC::Result result;
  get_request.need("age");
  EXPECT_EQ(-22,get_request.do_execute(result));
}

TEST_F(DTCClientAPITest, SELECT_EMPTY_TABLE)
{
  DTC::GetRequest get_request(&server);
  DTC::Result result;
  get_request.set_key(10000);
  EXPECT_EQ(0,get_request.do_execute(result));
}

TEST_F(DTCClientAPITest, SELECT_FIELD_TYPE)
{
  int retCode = -1;
  retCode = server.get_field_type("uid");
  if(retCode == 1)
      retCode = server.get_field_type("name");
  if(retCode == 5)
      retCode = server.get_field_type("city");
  if(retCode == 4)
      retCode = server.get_field_type("age");
  if(retCode == 1)
      retCode = server.get_field_type("sex");
  EXPECT_EQ(1,retCode);
}

TEST_F(DTCClientAPITest, SELECT_ADDRESS)
{
  EXPECT_STREQ("127.0.0.1:20015/tcp",server.get_address());
}

TEST_F(DTCClientAPITest, SELECT_TABLENAAE)
{
  EXPECT_STREQ("dtc_opensource",server.get_table_name());
}

TEST_F(DTCClientAPITest, SELECT_LIMIT)
{
  DTC::Result result;
  EXPECT_EQ(0,compare(1,server,result,true));
  EXPECT_EQ(2,result.get_num_row_size());
  for (int i = 0; i < result.get_num_row_size(); ++i) {
    result.fetch_row();
    EXPECT_EQ(data[i+2].uid,result.int_value("uid"));
    EXPECT_STREQ(data[i+2].name,result.binary_value("name"));
    EXPECT_STREQ(data[i+2].city,result.string_value("city"));
    EXPECT_EQ(data[i+2].age,result.int_value("age"));
    EXPECT_EQ(data[i+2].sex,result.int_value("sex"));
  }
}

TEST_F(DTCClientAPITest, SELECT_LIMIT_RESULT_NUM)
{
  DTC::Result result;
  EXPECT_EQ(0,compare(1,server,result,true));
  EXPECT_EQ(2,result.get_num_row_size());
}

TEST_F(DTCClientAPITest, SELECT_NO_LIMIT_RESULT_NUM)
{
  DTC::Result result;
  DTC::GetRequest get_request(&server);
  get_request.set_key(1);
  EXPECT_EQ(0,get_request.do_execute(result));
  EXPECT_EQ(5,result.get_total_rows_size());
}

TEST_F(DTCClientAPITest, SELECT_GET_RESULT_KEY)
{
  DTC::Result result;
  DTC::GetRequest get_request(&server);
  get_request.set_key(1);
  EXPECT_EQ(0,get_request.do_execute(result));
  EXPECT_EQ(1,result.int_key());
}

TEST_F(DTCClientAPITest, UPDATE_CORRENT_DATA) {
  DTC::UpdateRequest UpdateReq(&server);
  DTC::Result result;
  UpdateReq.set_key(1);
  UpdateReq.Set("name", "海绵宝宝");
  UpdateReq.Set("city", "大海");
  UpdateReq.Set("age", 5);
  UpdateReq.Set("sex", 1);
  EXPECT_EQ(0,UpdateReq.do_execute(result));
  EXPECT_EQ(5,result.get_affected_rows_size());
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    EXPECT_EQ(1,result.int_value("uid"));
    EXPECT_STREQ("海绵宝宝",result.binary_value("name"));
    EXPECT_STREQ("大海",result.string_value("city"));
    EXPECT_EQ(5,result.int_value("age"));
    EXPECT_EQ(1,result.int_value("sex"));
  }
}

TEST_F(DTCClientAPITest, UPDATE_KEY) {
  DTC::UpdateRequest UpdateReq(&server);
  DTC::Result result;
  UpdateReq.set_key(1);
  UpdateReq.Set("uid", 2);
  EXPECT_EQ(-2037,UpdateReq.do_execute(result));
}

TEST_F(DTCClientAPITest, UPDATE_DELETION_FIELD) {
  DTC::UpdateRequest UpdateReq(&server);
  DTC::Result result;
  UpdateReq.set_key(1);
  UpdateReq.Set("uid", 3);
  UpdateReq.Set("age", 11);
  EXPECT_EQ(-2037,UpdateReq.do_execute(result));
}

TEST_F(DTCClientAPITest, UPDATE_DELETION_KEY) {
  DTC::UpdateRequest UpdateReq(&server);
  DTC::Result result;
  UpdateReq.set_key(10000);
  EXPECT_EQ(-2002,UpdateReq.do_execute(result));
}

TEST_F(DTCClientAPITest, UPDATE_ONCE_MULTIPLE) {
  DTC::UpdateRequest UpdateReq(&server);
  DTC::Result result;

  UpdateReq.set_key(1);
  UpdateReq.Set("name", "派大星");
  UpdateReq.Set("city", "深海");
  UpdateReq.Set("age", 8);
  UpdateReq.Set("sex", 0);
  EXPECT_EQ(0,UpdateReq.do_execute(result));
  EXPECT_EQ(0,compare(1,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    EXPECT_EQ(1,result.int_value("uid"));
    EXPECT_STREQ("派大星",result.binary_value("name"));
    EXPECT_STREQ("深海",result.string_value("city"));
    EXPECT_EQ(8,result.int_value("age"));
    EXPECT_EQ(0,result.int_value("sex"));
  }
  
  UpdateReq.Reset();
  UpdateReq.set_key(2);
  UpdateReq.Set("name", "蟹老板");
  UpdateReq.Set("city", "大海");
  UpdateReq.Set("age", 12);
  UpdateReq.Set("sex", 1);
  EXPECT_EQ(0,UpdateReq.do_execute(result));
  EXPECT_EQ(0,compare(2,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    EXPECT_EQ(2,result.int_value("uid"));
    EXPECT_STREQ("蟹老板",result.binary_value("name"));
    EXPECT_STREQ("大海",result.string_value("city"));
    EXPECT_EQ(12,result.int_value("age"));
    EXPECT_EQ(1,result.int_value("sex"));
  }
}

TEST_F(DTCClientAPITest, UPDATE_EMPTY_TABLE) {
  DTC::UpdateRequest UpdateReq(&server);
  DTC::Result result;
  UpdateReq.set_key(-2);
  EXPECT_EQ(-2002,UpdateReq.do_execute(result));
}

TEST_F(DTCClientAPITest, SELECT_ADD_SUB) {

  DTC::UpdateRequest UpdateReq(&server);
  DTC::Result result;
  UpdateReq.set_key(2);
  UpdateReq.Add("age", 3);
  UpdateReq.Sub("sex", 1);
  EXPECT_EQ(0,UpdateReq.do_execute(result));
  EXPECT_EQ(0,compare(2,server,result));
  for (int i = 1; i <= result.get_num_row_size(); ++i) 
  {
    result.fetch_row();
    EXPECT_EQ(2,result.int_value("uid"));
    EXPECT_STREQ("蟹老板",result.binary_value("name"));
    EXPECT_STREQ("大海",result.string_value("city"));
    EXPECT_EQ(15,result.int_value("age"));
    EXPECT_EQ(0,result.int_value("sex"));
  }
}

TEST_F(DTCClientAPITest, DELETE_EXISTENCE) {
  DTC::DeleteRequest deleteReq(&server);
  DTC::Result result;
  deleteReq.set_key(1);
  EXPECT_EQ(0,deleteReq.do_execute(result));
  EXPECT_EQ(0,result.get_total_rows_size());
}

TEST_F(DTCClientAPITest, DELETE_NO_EXISTENCE) {
  DTC::DeleteRequest deleteReq(&server);
  DTC::Result result;
  deleteReq.set_key(-1);
  EXPECT_EQ(0,deleteReq.do_execute(result));
}

TEST_F(DTCClientAPITest, DELETE_BAD_TYPE_KEY) {
  DTC::DeleteRequest deleteReq(&server);
  DTC::Result result;
  deleteReq.set_key("jd");
  EXPECT_EQ(-2024,deleteReq.do_execute(result));
}

TEST_F(DTCClientAPITest, DELETE_NO_KEY_FIELD) {

  DTC::DeleteRequest deleteReq(&server);
  DTC::Result result;
  deleteReq.Set("age", 12);
  EXPECT_EQ(-2013,deleteReq.do_execute(result));
}

TEST_F(DTCClientAPITest, DELETE_EMPTY_TABLE) {
  DTC::DeleteRequest deleteReq(&server);
  DTC::Result result;
  deleteReq.set_key(10000);
  EXPECT_EQ(0,deleteReq.do_execute(result));
}

TEST_F(DTCClientAPITest, DELETE_ONCE_MULTIPLE) {
  DTC::DeleteRequest deleteReq(&server);
  DTC::Result result;
  deleteReq.set_key(2);
  EXPECT_EQ(0,deleteReq.do_execute(result));
  EXPECT_EQ(0,result.get_total_rows_size());
  deleteReq.Reset();
  deleteReq.set_key(3);
  EXPECT_EQ(0,deleteReq.do_execute(result));
  EXPECT_EQ(0,result.get_total_rows_size());
}

int main(int argc, char *argv[])
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}






