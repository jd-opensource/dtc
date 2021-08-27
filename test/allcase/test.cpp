#include<stdio.h>
#include<stdlib.h>
#include<string>
#include<stdint.h>
#include<unistd.h>

#include "dtcapi.h"

enum TypeIns{
    E_INSERT_CORRENT_DATA = 1,  
    E_INSERT_DELETION_FIELD, 
    E_INSERT_DELETION_KEY,
    E_INSERT_BAD_TYPE,
    E_INSERT_BAD_ORDER,
    E_INSERT_SAME,
    E_INSERT_ONCE_MULTIPLE,

    E_TOTAL_NUM_INS
};

enum TypeDel{
    E_DELETE_EXISTENCE =10,
    E_DELETE_NO_EXISTENCE,
    E_DELETE_BAD_TYPE_KEY,
    E_DELETE_NO_KEY_FIELD,
    E_DELETE_EMPTY_TABLE,
    E_DELETE_ONCE_MULTIPLE,

    E_TOTAL_NUM_DEL
};

enum TypeUpd{
    E_UPDATE_CORRENT_DATA = 20,
    E_UPDATE_KEY,
    E_UPDATE_DELETION_FIELD,
    E_UPDATE_DELETION_KEY,
    E_UPDATE_ONCE_MULTIPLE,
    E_UPDATE_EMPTY_TABLE,
    E_SELECT_ADD_SUB,
    E_TOTAL_NUM_UPD
};

enum TypeSel{
    E_SELECT_CORRENT_KEY = 30,
    E_SELECT_BAD_KEY,
    E_SELECT_BAD_TYPE_KEY,
    E_SELECT_NO_KEY_FIELD,
    E_SELECT_ONCE_MULTIPLE,
    E_SELECT_EMPTY_TABLE,
    E_SELECT_FIELD_TYPE,
    E_SELECT_ADDRESS,
    E_SELECT_TABLENAAE,

    E_SELECT_LIMIT,  
    E_SELECT_LIMIT_RESULT_NUM,
    E_SELECT_NO_LIMIT_RESULT_NUM,

    E_TOTAL_NUM_SEL
};


void insert(DTC::Server server,TypeIns type)
{
    int retCode = 0;

    DTC::InsertRequest insertReq(&server); 
    DTC::Result stResult;
    switch (type)
    {
    case E_INSERT_CORRENT_DATA:
    case E_INSERT_SAME:
         /* 绑定插入的字段和数据 */
         insertReq.set_key(1);
	     insertReq.Set("uid", 1);
	     insertReq.Set("name", "汪汪");
	     insertReq.Set("city", "上海");
	     insertReq.Set("age", 3);
	     insertReq.Set("sex", 1);
        break;
    case E_INSERT_DELETION_FIELD:
         insertReq.set_key(1);
         insertReq.Set("uid", 1);
	     insertReq.Set("name", "喵喵");
         break;
    case E_INSERT_DELETION_KEY:
	     insertReq.Set("city", "杭州");
         insertReq.Set("age", 9);
         break;
    case E_INSERT_BAD_TYPE:
         insertReq.set_key("jd");
         break;
    case E_INSERT_BAD_ORDER:
         insertReq.set_key(1);
         insertReq.Set("age", 2);
         insertReq.Set("name", "呱呱");
         insertReq.Set("uid", 1);
	     insertReq.Set("city", "上海");	    
	     insertReq.Set("sex", 0);
         break;
    case E_INSERT_ONCE_MULTIPLE:
         insertReq.set_key(1);
	     insertReq.Set("uid", 1);
	     insertReq.Set("name", "咩咩");
	     insertReq.Set("city", "西安");
	     insertReq.Set("age", 7);
	     insertReq.Set("sex", 1);
         insertReq.do_execute(stResult);   
         insertReq.Reset();
         insertReq.set_key(2);
	     insertReq.Set("uid", 2);
	     insertReq.Set("name", "布谷");
	     insertReq.Set("city", "福建");
	     insertReq.Set("age", 11);
	     insertReq.Set("sex", 0);
         break;
    default:
        break;
    }
	retCode = insertReq.do_execute(stResult);
	if(retCode != 0)
		printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode, stResult.get_error_message(), stResult.get_error_from());
    else
        printf("insert success!\n");
    printf("==========================================================\n");
}

void delet(DTC::Server server,TypeDel type)
{

	int retCode = 0;

    DTC::DeleteRequest deleteReq(&server);
    DTC::Result stResult;
    switch (type)
    {
    case E_DELETE_EXISTENCE:
         deleteReq.set_key(1);
         break;
    case E_DELETE_NO_EXISTENCE:
         deleteReq.set_key(-1);
         break;
    case E_DELETE_BAD_TYPE_KEY:
         deleteReq.set_key("jd");
         break;
    case E_DELETE_NO_KEY_FIELD:
	     deleteReq.Set("age", 12);
         break;
    case E_DELETE_EMPTY_TABLE:
         deleteReq.set_key(10000);
         break;   
    case E_DELETE_ONCE_MULTIPLE:
         deleteReq.set_key(2);
         deleteReq.do_execute(stResult);
         deleteReq.Reset();
         deleteReq.set_key(3);
         break;
    default:
        break;
    }
	retCode = deleteReq.do_execute(stResult);
	if(retCode == 0)
		printf("delete success!\n");
	if(retCode != 0)
		printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode, stResult.get_error_message(), stResult.get_error_from());
    printf("==========================================================\n");
}
void updata(DTC::Server server,TypeUpd type)
{
    int retCode = 0;

	DTC::UpdateRequest UpdateReq(&server);
    DTC::Result stResult;
	UpdateReq.set_key(1);

    switch (type)
    {
    case E_UPDATE_CORRENT_DATA:
         UpdateReq.Set("name", "海绵宝宝");
         UpdateReq.Set("city", "大海");
         UpdateReq.Set("age", 5);
         UpdateReq.Set("sex", 1);
         break;
    case E_UPDATE_KEY:
         UpdateReq.Set("uid", 2);
         break;
    case E_UPDATE_DELETION_FIELD:
         UpdateReq.Set("uid", 3);
         UpdateReq.Set("age", 11);
         break;
    case E_UPDATE_EMPTY_TABLE:
         UpdateReq.set_key(-2);
         break;
    case E_UPDATE_DELETION_KEY:
         UpdateReq.set_key(10000);
         break;
    case E_UPDATE_ONCE_MULTIPLE: 
         UpdateReq.set_key(1);
         UpdateReq.Set("name", "派大星");
         UpdateReq.Set("city", "深海");
         UpdateReq.Set("age", 8);
         UpdateReq.Set("sex", 0);
         UpdateReq.do_execute(stResult);
         UpdateReq.Reset();
         UpdateReq.set_key(2);
         UpdateReq.Set("name", "蟹老板");
         UpdateReq.Set("city", "大海");
         UpdateReq.Set("age", 12);
         UpdateReq.Set("sex", 1);
         break;
    case E_SELECT_ADD_SUB:
         UpdateReq.set_key(1);
         UpdateReq.Add("age", 8);
         UpdateReq.Sub("sex", 1);
    default:
        break;
    }
	retCode = UpdateReq.do_execute(stResult);
	if(retCode != 0) 
		printf("retCode:%d, errmsg:%s, errfrom:%s\n", retCode, stResult.get_error_message(), stResult.get_error_from());
    else
		printf("update success!\n");
    /* 查询修改影响的行数 */
    printf("affected rows size %d\n",stResult.get_affected_rows_size());
    printf("==========================================================\n");
}

void select(DTC::Server server,TypeSel type)
{
    /* 直接构造指定操作的对象：查询操作 */
    DTC::GetRequest get_request(&server); 
    int uid;
    /* 执行的结果:包含错误信息以及返回的row数据 */
    DTC::Result result; 
	int iRet;
    switch (type)
    {
    case E_SELECT_CORRENT_KEY:
         get_request.set_key(1);
         uid = 1;
         break;
    case E_SELECT_BAD_KEY:
         get_request.set_key(-1);
         uid = -1;
         break;
    case E_SELECT_BAD_TYPE_KEY:
         get_request.set_key("jd");
         break;
    case E_SELECT_NO_KEY_FIELD:
         get_request.need("age", 7);
         break;
    case E_SELECT_ONCE_MULTIPLE:
         get_request.set_key(2);
         uid = 2;
         break;
    case E_SELECT_EMPTY_TABLE:
         get_request.set_key(10000);
         uid = 10000;
         break;
    case E_SELECT_FIELD_TYPE:    
         /**
	      * Signed = 1,	     Signed Integer
	      * Unsigned = 2,     Unsigned Integer
	      * FloatPoint = 3    FloatPoint
	      * String = 4,	     String, case insensitive, null ended
	      * Binary = 5,	     opaque binary data
	      */  
	     printf("FieldType uid: %d\n",server.get_field_type("uid"));
	     printf("FieldType name: %d\n",server.get_field_type("name"));
	     printf("FieldType city: %d\n",server.get_field_type("city"));  
	     printf("FieldType age: %d\n",server.get_field_type("age"));
	     printf("FieldType sex: %d\n",server.get_field_type("sex"));
         printf("==========================================================\n");
         return;
         break;
    case E_SELECT_ADDRESS:
         /* 返回服务器端ip地址 */
         printf("ip_address: %s\n",server.get_address());
         printf("==========================================================\n");
         return;
         break;
    case E_SELECT_TABLENAAE:
         /* 返回表名 */
         printf("tablename: %s\n",server.get_table_name());
         printf("==========================================================\n");
         return;
         break;
    case E_SELECT_LIMIT:
    case E_SELECT_LIMIT_RESULT_NUM:
         get_request.set_key(1);
         /* 返回结构集开始位置 以及结果行数 */
         get_request.limit(2, 2); 
         uid = 1;
         break;
    case E_SELECT_NO_LIMIT_RESULT_NUM:
         get_request.set_key(1);
         get_request.do_execute(result); 
         printf("total nubrow:%d\n", result.get_total_rows_size()); 
         uid = 1;
         printf("==========================================================\n");
         return;
         break;
    default:
        break;
    }

    /* 设置需要select的字段 */
	if(iRet == 0)
		iRet = get_request.need("uid");
    if(iRet == 0)
    	iRet = get_request.need("name");
    if(iRet == 0)
    	iRet = get_request.need("city");
    if(iRet == 0)
        iRet = get_request.need("age");
    if(iRet == 0)
    	iRet = get_request.need("sex");
    if(iRet != 0)
    { 
		printf("get-req need error: %d", iRet);
		fflush(stdout); 
    } 
  
	/* 提交请求，执行结果存在Result对象里 */
    iRet = get_request.do_execute(result);  
	/* 如果出错,则输出错误码、错误阶段，错误信息，stResult.get_error_from(), result.get_error_message() 这两个错误信息很重要，一定要打印出来，方便定位问题 */
    if(iRet != 0)
    { 
		printf ("uin[%u] dtc execute get error: %d, error_from:%s, msg:%s\n",
			uid,  // 出错的key是多少
			iRet, // 错误码为多少
			result.get_error_from(),   // 返回错误阶段
			result.get_error_message() // 返回错误信息
			);
		fflush(stdout);
    } 
    /* 数据不存在 */
    if(result.get_num_row_size() <= 0)
		printf("uin[%u] data not exist.\n", uid);

    /* 读取结果的Key值 */
	printf("result key: %d\n", result.int_key());  

    /* 输出结果的行数 */
	printf("NumRows:%d\n", result.get_num_row_size());
	
	/* 输出结果 */
	for(int i=0;i<=result.get_num_row_size();++i)
	{
		/* 读取一行数据 */
		iRet = result.fetch_row();
		if(iRet < 0)
		{
			printf ("uid[%lu] dtc fetch row error: %d\n", uid, iRet);
			fflush(stdout);
		}
		/* 如果一切正确，则可以输出数据了 */
        /* 输出int类型的数据 */
		printf("uid: %d\n", result.int_value("uid"));
        /* 输出binary类型的数据 */
		printf("name: %s\n", result.binary_value("name"));
        /* 输出string类型的数据 */
		printf("city: %s\n", result.string_value("city"));
		printf("age:%d\n", result.int_value("age"));
		printf("sex:%d\n",result.int_value("sex"));
	}
    printf("==========================================================\n");
}

int main(int argc,char* argv[])
{
    std::string ip;
    ip = std::string(argv[1]);

     /* 只要server不析构，后台会保持长连接 */
	DTC::Server server; 

    /* 设置的dtc的ip和端口 */
	server.set_address(ip.c_str(), "20015");
    /* 设置网络超时时间,单次网络IO的超时,单位秒 */
	server.SetTimeout(5);
    /* 设置访问码 AccessToken，在申请dtc实例的时候网站端会生成 */
	server.set_accesskey("000022907e64e117fa92f892a85307782a68afc6"); 
    /* 设置dtc的表名 */
    server.set_table_name("dtc_opensource");
    /* 声明key类型 */
	server.int_key(); 

    int i;
    for(i = E_INSERT_CORRENT_DATA;i < E_TOTAL_NUM_INS;i++)
    {
        insert(server,(TypeIns)i);
    }
    select(server,E_SELECT_CORRENT_KEY);
    for(i = E_UPDATE_CORRENT_DATA;i < E_TOTAL_NUM_UPD;i++)
    {
        updata(server,(TypeUpd)i);
    }
    for(i = E_SELECT_CORRENT_KEY;i < E_TOTAL_NUM_SEL;i++)
    {
        select(server,(TypeSel)i);
    }
    for(int i = E_DELETE_EXISTENCE;i < E_TOTAL_NUM_DEL;i++)
    {
        delet(server,(TypeDel)i);
    }
    return 0;
}



