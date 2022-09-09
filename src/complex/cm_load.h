#ifndef _DBP_DBCONFIG_H_
#define _DBP_DBCONFIG_H_

#include "global.h"
#include "yaml-cpp/yaml.h"
#include "cm_conn.h"

class TableInfo;

int GetTableIdx(void* Key, int FieldType, TableInfo* dbConfig);

class ConfigHelper 
{
private:
	std::string m_data;
	YAML::Node dtc;

	void load_yaml_fields(YAML::Node node);

public:
	DBHost hot_instance;
	DBHost full_instance;

public:
	ConfigHelper ();
	~ConfigHelper ();

    int GetIntValue(const char* key, int default_value = 0);
    std::string GetStringValue(const char* key, std::string default_value = "");
	std::vector<int> GetIntArray(const char* key);

	bool load_dtc_config(std::string conf_file);
	
	bool load_hot_inst_info();
	bool load_full_inst_info();

	void load_layered_info();

	YAML::Node get_conf() { return dtc;}
	std::map<std::string, std::string> table_type_info;
};

#endif