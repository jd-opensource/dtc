#ifndef DATA_CONF_TEST_H_
#define DATA_CONF_TEST_H_

#include "../data_conf.h"
#include "gtest/gtest.h"

class DataConfTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        data_conf_ = new DataConf();
    }
    static void TearDownTestCase() {
        if(NULL != data_conf_){
            delete data_conf_;
        }
    }
    static DataConf* data_conf_;
};

DataConf* DataConfTest::data_conf_ = NULL;

TEST_F(DataConfTest, CONF_) {
    int argc = 1;
    char* argv[] = {};
    int ret = data_conf_->LoadConfig(argc, argv);
    EXPECT_EQ(0, ret);
    ConfigParam config_param;
    ret = data_conf_->ParseConfig(config_param);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(2, config_param.single_query_cnt_);
    EXPECT_EQ("status = 0", config_param.data_rule_);
    EXPECT_EQ("delete", config_param.operate_type_);
    EXPECT_EQ("uid", config_param.key_field_name_);
    EXPECT_EQ("dtc_opensource", config_param.table_name_);
}

#endif