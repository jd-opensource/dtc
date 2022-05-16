#ifndef DATA_MANAGER_MOCK_TEST_H_
#define DATA_MANAGER_MOCK_TEST_H_

#include "unittest_comm.h"
#include "../data_manager.h"

UNITEST_NAMESPACE_BEGIN
class MockDataManager : public DataManager{
public:
    MockDataManager() : DataManager(){
    }
    MockDataManager(const ConfigParam& config_param) : DataManager(config_param) {
    };
    ~MockDataManager(){
    }
    MOCK_METHOD(int, GetLastId, (uint64_t& last_delete_id, std::string& last_invisible_time));
    MOCK_METHOD(int, DoQuery, (const std::string& query_sql, std::vector<QueryInfo>& query_info_vec));
    MOCK_METHOD(int, DoDelete, (const std::string& delete_sql));
    MOCK_METHOD(int, UpdateLastDeleteId, ());
};

class DataManagerTest : public testing::Test {
protected:
    DataManagerTest():data_manager_(&data_manager_mock_){};

    DataManager* data_manager_; 
    MockDataManager data_manager_mock_;
};

TEST_F(DataManagerTest , DoProcessTest){
    EXPECT_CALL(data_manager_mock_ , GetLastId(testing::_, testing::_)).Times(AnyNumber())
        .WillOnce(Return(0)).WillOnce(Return(1))
        .WillRepeatedly(Return(0));

    std::vector<QueryInfo> query_info_vec;
    QueryInfo info;
    info.id = 1;
    info.invisible_time = "2022-03-01 15:00:43";
    info.key_info = "1";
    query_info_vec.push_back(info);
    EXPECT_CALL(data_manager_mock_ , DoQuery(testing::_, testing::_)).Times(AnyNumber())
        .WillOnce(Return(1))
        .WillOnce(DoAll(SetArgReferee<1>(query_info_vec), Return(0)))
        .WillRepeatedly(Return(0));

    EXPECT_CALL(data_manager_mock_ , DoDelete(testing::_)).Times(AnyNumber())
        .WillOnce(Return(1)).WillOnce(Return(0)).WillOnce(Return(1))
        .WillRepeatedly(Return(0));

    EXPECT_CALL(data_manager_mock_ , UpdateLastDeleteId()).Times(AnyNumber())
        .WillOnce(Return(1)).WillOnce(Return(0))
        .WillRepeatedly(Return(0));
    data_manager_->SetTimeRule("0 */1 * * * ?");
    data_manager_->SetDataRule("status = 0");
    uint64_t last_delete_id;
    std::string last_invisible_time;
    printf("1\n");
    EXPECT_NE(0, data_manager_->DoTaskOnce());
    printf("2\n");
    EXPECT_NE(0, data_manager_->DoTaskOnce());
    printf("3\n");
    EXPECT_NE(0, data_manager_->DoTaskOnce());
    printf("4\n");
    EXPECT_EQ(0, data_manager_->DoTaskOnce());
}
UNITEST_NAMESPACE_END
#endif
