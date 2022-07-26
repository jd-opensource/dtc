#include <stdio.h>
#include <iostream>
#include "log.h"
#include "yaml-cpp/yaml.h"
#include <vector>
#include <map>

using namespace std;

char conf_file[256] = {0};
YAML::Node dtc_config;
std::map<std::string, std::vector<YAML::Node>> dbmap;

int load_dtc_config(int argc, char *argv[])
{
    int c;
    strcpy(conf_file, "/etc/dtc/dtc.yaml");

    while ((c = getopt(argc, argv, "c:")) != -1) {
        switch (c) {
        case 'c':
            log4cplus_info("conf file:%s", optarg);
            strncpy(conf_file, optarg, sizeof(conf_file) - 1);
            break;
        }
    }

    try {
        dtc_config = YAML::LoadFile(conf_file);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s, %s\n", e.what(), conf_file);
		return -1;
	}

    if(!dtc_config)
        return -1;

    return 0;
}

int load_node_to_map()
{
    //hot
    std::vector<YAML::Node> vec = 
        dbmap[dtc_config["primary"]["hot"]["logic"]["db"].as<string>()];
    vec.push_back(dtc_config["primary"]["hot"]);
    dbmap[dtc_config["primary"]["hot"]["logic"]["db"].as<string>()] = vec;

    //full
    vec = dbmap[dtc_config["primary"]["full"]["logic"]["db"].as<string>()];
    vec.push_back(dtc_config["primary"]["full"]);
    dbmap[dtc_config["primary"]["full"]["logic"]["db"].as<string>()] = vec;

    //extension
    for(int i = 0; i < dtc_config["extension"].size(); i++)
    {
        vec = dbmap[dtc_config["extension"][i]["logic"]["db"].as<string>()];
        vec.push_back(dtc_config["extension"][i]);
        dbmap[dtc_config["extension"][i]["logic"]["db"].as<string>()] = vec;
    }

    return 0;
}

int yaml_dump_schema_name(FILE *fp, std::string logic_db_name)
{
    fprintf(fp, "schemaName: %s\n", logic_db_name.c_str());
    return 0;
}

int yaml_dump_data_sources(FILE *fp, std::vector<YAML::Node> vec)
{
    fprintf(fp, "dataSources:\n");
    std::vector<YAML::Node>::iterator vt;
    for(vt = vec.begin(); vt != vec.end(); vt++)
    {
        YAML::Node node = *vt;
        fprintf(fp, "  %s:\n", node["logic"]["db"].as<string>().c_str());

        fprintf(fp, "    url: jdbc:mysql://%s/%s?serverTimezone=UTC&useSSL=false&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8\n",
            node["real"]["addr"].as<string>().c_str(),
            node["real"]["db"].as<string>().c_str());
        fprintf(fp, "    username: %s\n", node["real"]["user"].as<string>().c_str());
        fprintf(fp, "    password: %s\n", node["real"]["pwd"].as<string>().c_str());

        fprintf(fp, "    connectionTimeoutMilliseconds: %d\n", 30000);
        fprintf(fp, "    idleTimeoutMilliseconds: %d\n", 60000);
        fprintf(fp, "    maxLifetimeMilliseconds: %d\n", 1800000);
        fprintf(fp, "    maxPoolSize: %d\n", 50);
    }
    return 0;
}

std::string get_merge_string(YAML::Node node)
{
    std::string str = "";
    if(!node)
        return str;

    for(int i = 0; i < node.size(); i++)
    {
        str += node[i].as<string>();
    }

    return str;
}

std::string yaml_dump_actual_data_nodes(YAML::Node node)
{
    std::string str = "";
    //single db
    if(node["real"].size() == 1 && node["real"][0]["db"].IsScalar())    
    {
        str += node["real"][0]["db"].as<string>();
    }
    else    //multi db
    {
        str += get_merge_string(node["real"][0]["db"]["prefix"]);
        char sztmp[200] = {0};
        sprintf(sztmp, "${%d..%d}", 
            node["real"][0]["db"]["start"].as<int>(),
            node["real"][node["real"].size()-1]["db"]["last"].as<int>());
        str += sztmp;
    }

    str += ".";

    //single table
    if(!node["sharding"])
    {
        str += node["logic"]["table"].as<string>();
    }
    else    //multi table
    {
        str += get_merge_string(node["sharding"]["table"]["prefix"]);
        char sztmp[200] = {0};
        sprintf(sztmp, "${%d..%d}", 
            node["sharding"]["table"]["start"].as<int>(),
            node["sharding"]["table"]["last"].as<int>());
        str += sztmp;
    }

    return str;
}

int yaml_dump_sharding_rule(FILE *fp, std::vector<YAML::Node> vec)
{
    fprintf(fp, "shardingRule:\n");
    fprintf(fp, "  tables:\n");
    std::string binding_table = "";
    std::vector<YAML::Node>::iterator vt;
    for(vt = vec.begin(); vt != vec.end(); vt++)
    {
        YAML::Node node = *vt;
        int dbcount = 1;
        std::string tbname = node["logic"]["table"].as<string>();
        binding_table += tbname;
        if(vt + 1 != vec.end())
            binding_table += ",";
        fprintf(fp, "    %s:\n", tbname.c_str());

        fprintf(fp, "      actualDataNodes: %s\n", yaml_dump_actual_data_nodes(node).c_str());

        if(node["sharding"])
        {
            std::string key = node["sharding"]["key"].as<string>();

            if(node["real"].size() > 1 || node["real"][0]["db"].IsMap())
            {
                dbcount = node["real"][node["real"].size()-1]["db"]["last"].as<int>() - 
                    node["real"][0]["db"]["start"].as<int>() + 1;
                fprintf(fp, "      databaseStrategy:\n");
                fprintf(fp, "        inline:\n");
                fprintf(fp, "          shardingColumn: %s\n", key.c_str());
                fprintf(fp, "          algorithmExpression: %s${(%s%%%d)}\n",
                    get_merge_string(node["real"][0]["db"]["prefix"]).c_str(),
                    key.c_str(),
                    dbcount);
            }
            
            fprintf(fp, "      tableStrategy:\n");
            fprintf(fp, "        inline:\n");
            fprintf(fp, "          shardingColumn: %s\n", key.c_str());
            fprintf(fp, "          algorithmExpression: %s${((%s/%d).longValue()%%%d)}\n",
                get_merge_string(node["sharding"]["table"]["prefix"]).c_str(),
                key.c_str(),
                dbcount,
                node["sharding"]["table"]["last"].as<int>() -
                node["sharding"]["table"]["start"].as<int>() + 1);
        }
    }

    fprintf(fp, "  bindingTables:\n");
    fprintf(fp, "    - %s:\n", binding_table.c_str());

    return 0;
}



int dump_single_conf_file(std::string logic_db_name, std::vector<YAML::Node> vec)
{
    std::string filename = logic_db_name + ".yaml";
    FILE *fp = fopen(filename.c_str(), "w");
    if (fp == NULL)
        return -1;

    yaml_dump_schema_name(fp, logic_db_name);
    yaml_dump_data_sources(fp, vec);
    yaml_dump_sharding_rule(fp, vec);
    
    fclose(fp);
    return 0;
}

int dump_shardingsphere_conf_files()
{
    std::map<std::string, std::vector<YAML::Node>>::iterator it;
    for (it = dbmap.begin(); it != dbmap.end(); it++) 
    {
        std::string logic_db_name = (*it).first;
        std::vector<YAML::Node> vec = (*it).second;
        dump_single_conf_file(logic_db_name, vec);
        
    }

    return 0;
}

int main(int argc, char* argv[])
{
    init_log4cplus();
    log4cplus_info("************* conf-gen-utils *************");

    if(load_dtc_config(argc, argv) < 0)
        return 0;

    load_node_to_map();

    dump_shardingsphere_conf_files();

    return 0;
}
