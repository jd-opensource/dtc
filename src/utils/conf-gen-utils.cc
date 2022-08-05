#include <stdio.h>
#include <iostream>
#include "log.h"
#include "yaml-cpp/yaml.h"
#include <vector>
#include <map>

using namespace std;
#define ROOT_PATH "/etc/dtc/"

char conf_file[256] = {0};
YAML::Node dtc_config;
std::map<std::string, std::vector<YAML::Node>> dbmap;
std::string get_merge_string(YAML::Node node);
std::map<std::string, std::string> algorithm;

int load_dtc_config(int argc, char *argv[])
{
    int c;
    strcpy(conf_file, ROOT_PATH);
    strcat(conf_file, "dtc.yaml");

    while ((c = getopt(argc, argv, "c:")) != -1) {
        switch (c) {
        case 'c':
            log4cplus_info("conf file:%s", optarg);
            strncpy(conf_file, optarg, sizeof(conf_file) - 1);
            break;
        }
    }

    try {
        log4cplus_info("loading file: %s", conf_file);
        dtc_config = YAML::LoadFile(conf_file);
	} catch (const YAML::Exception &e) {
		log4cplus_error("config file error:%s, %s\n", e.what(), conf_file);
		return -1;
	}

    if(!dtc_config)
        return -1;

    log4cplus_info("loading conf file successfully.");
    return 0;
}

int load_node_to_map()
{
    log4cplus_info("loading node to map.");
    std::vector<YAML::Node> vec;
    //hot
    if(dtc_config["primary"]["hot"])
    {
        log4cplus_info("loading hot.");
        vec = 
            dbmap[dtc_config["primary"]["hot"]["logic"]["db"].as<string>()];
        vec.push_back(dtc_config["primary"]["hot"]);
        dbmap[dtc_config["primary"]["hot"]["logic"]["db"].as<string>()] = vec;
    }

    //full
    if(dtc_config["primary"]["full"])
    {
        log4cplus_info("loading full.");
        vec = dbmap[dtc_config["primary"]["full"]["logic"]["db"].as<string>()];
        vec.push_back(dtc_config["primary"]["full"]);
        dbmap[dtc_config["primary"]["full"]["logic"]["db"].as<string>()] = vec;
    }

    //extension
    if(dtc_config["extension"])
    {
        log4cplus_info("loading extension.");
        for(int i = 0; i < dtc_config["extension"].size(); i++)
        {
            vec = dbmap[dtc_config["extension"][i]["logic"]["db"].as<string>()];
            vec.push_back(dtc_config["extension"][i]);
            dbmap[dtc_config["extension"][i]["logic"]["db"].as<string>()] = vec;
        }
    }

    log4cplus_info("loading node to map finished.");
    return 0;
}

int yaml_dump_schema_name(FILE *fp, std::string logic_db_name)
{
    fprintf(fp, "databaseName: %s\n", logic_db_name.c_str());
    return 0;
}

int yaml_dump_data_sources(FILE *fp, std::vector<YAML::Node> vec)
{
    fprintf(fp, "dataSources:\n");
    std::vector<YAML::Node>::iterator vt;
    for(vt = vec.begin(); vt != vec.end(); vt++)
    {
        YAML::Node node = *vt;
        for(int i = 0; i < node["real"].size(); i++)
        {
            if(node["real"][i]["db"].IsScalar())  //single db
            {
                fprintf(fp, "  %s:\n", node["real"][i]["db"].as<string>().c_str());
                fprintf(fp, "    url: jdbc:mysql://%s/%s?serverTimezone=UTC&useSSL=false&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8\n",
                    node["real"][i]["addr"].as<string>().c_str(), node["real"][i]["db"].as<string>().c_str());

                fprintf(fp, "    username: %s\n", node["real"][i]["user"].as<string>().c_str());
                fprintf(fp, "    password: %s\n", node["real"][i]["pwd"].as<string>().c_str());
                fprintf(fp, "    connectionTimeoutMilliseconds: %d\n", 30000);
                fprintf(fp, "    idleTimeoutMilliseconds: %d\n", 60000);
                fprintf(fp, "    maxLifetimeMilliseconds: %d\n", 1800000);
                fprintf(fp, "    maxPoolSize: %d\n", 50);
                fprintf(fp, "    minPoolSize: %d\n", 1);
            }
            else    //multi db
            {
                for(int j = node["real"][i]["db"]["start"].as<int>(); j <= node["real"][i]["db"]["last"].as<int>(); j++)
                {
                    char szdb[250] = {0};
                    sprintf(szdb, "%s%d", get_merge_string(node["real"][i]["db"]["prefix"]).c_str(), j);
                    fprintf(fp, "  %s:\n", szdb);

                    fprintf(fp, "    url: jdbc:mysql://%s/%s?serverTimezone=UTC&useSSL=false&zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8\n",
                        node["real"][i]["addr"].as<string>().c_str(), szdb);

                    fprintf(fp, "    username: %s\n", node["real"][i]["user"].as<string>().c_str());
                    fprintf(fp, "    password: %s\n", node["real"][i]["pwd"].as<string>().c_str());
                    fprintf(fp, "    connectionTimeoutMilliseconds: %d\n", 30000);
                    fprintf(fp, "    idleTimeoutMilliseconds: %d\n", 60000);
                    fprintf(fp, "    maxLifetimeMilliseconds: %d\n", 1800000);
                    fprintf(fp, "    maxPoolSize: %d\n", 50);
                    fprintf(fp, "    minPoolSize: %d\n", 1);
                }
            }
            
            
        }


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
        sprintf(sztmp, "{%d..%d}", 
            node["sharding"]["table"]["start"].as<int>(),
            node["sharding"]["table"]["last"].as<int>());
        str += sztmp;
    }

    return str;
}

int yaml_dump_sharding_rule(FILE *fp, std::vector<YAML::Node> vec)
{
    fprintf(fp, "rules:\n");
    fprintf(fp, "- !SHARDING\n");
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
                fprintf(fp, "        standard:\n");
                fprintf(fp, "          shardingColumn: %s\n", key.c_str());
                char szname[1024] = {0};
                sprintf(szname, "%s_db_inline", tbname.c_str());
                fprintf(fp, "          shardingAlgorithmName: %s\n", szname);

                char sztemp[1024] = {0};
                sprintf(sztemp, "$%s{(%s%%%d)}", 
                    get_merge_string(node["real"][0]["db"]["prefix"]).c_str(),
                    key.c_str(),
                    dbcount);
                algorithm[szname] = sztemp;
            }
            
            fprintf(fp, "      tableStrategy:\n");
            fprintf(fp, "        standard:\n");
            fprintf(fp, "          shardingColumn: %s\n", key.c_str());
            char szname[1024] = {0};
            sprintf(szname, "%s_tb_inline", tbname.c_str());
            fprintf(fp, "          shardingAlgorithmName: %s\n", szname);
            char sztemp[1024] = {0};
            sprintf(sztemp, "%s${((%s/%d).longValue()%%%d)}", 
                get_merge_string(node["sharding"]["table"]["prefix"]).c_str(),
                key.c_str(),
                dbcount,
                node["sharding"]["table"]["last"].as<int>() -
                node["sharding"]["table"]["start"].as<int>() + 1);
            algorithm[szname] = sztemp;

            fprintf(fp, "      keyGenerateStrategy:\n");
            fprintf(fp, "        column: %s\n", key.c_str());
            fprintf(fp, "        keyGeneratorName: snowflake\n");
        }
    }

    fprintf(fp, "  shardingAlgorithms:\n");
    std::map<std::string, std::string>::iterator it;
    for (it = algorithm.begin(); it != algorithm.end(); it++) 
    {
        fprintf(fp, "    %s:\n", (*it).first.c_str());
        fprintf(fp, "      type: INLINE\n");
        fprintf(fp, "      props:\n");
        fprintf(fp, "        algorithm-expression: %s\n", (*it).second.c_str());
    }

    fprintf(fp, "  bindingTables:\n");
    fprintf(fp, "    - %s\n", binding_table.c_str());

    return 0;
}



int dump_single_conf_file(std::string logic_db_name, std::vector<YAML::Node> vec)
{
    std::string filename = string(ROOT_PATH) + string("config-") + logic_db_name + ".yaml";
    FILE *fp = fopen(filename.c_str(), "w");
    if (fp == NULL)
        return -1;
    yaml_dump_schema_name(fp, logic_db_name);
    yaml_dump_data_sources(fp, vec);
    yaml_dump_sharding_rule(fp, vec);
    fclose(fp);
    log4cplus_info("generating new conf file:%s", filename.c_str());
    return 0;
}

int dump_shardingsphere_conf_files()
{
    log4cplus_info("dumping ss conf files.");
    std::map<std::string, std::vector<YAML::Node>>::iterator it;
    for (it = dbmap.begin(); it != dbmap.end(); it++) 
    {
        std::string logic_db_name = (*it).first;
        std::vector<YAML::Node> vec = (*it).second;
        dump_single_conf_file(logic_db_name, vec);        
    }

    log4cplus_info("dumping ss conf files finished.");
    return 0;
}

void delete_all_old_yaml()
{
    std::string cmd = "rm -rf ";
    cmd += ROOT_PATH;
    cmd += "config-*.yaml";
    log4cplus_info("cmd: %s", cmd.c_str());
    system(cmd.c_str());
}

void dump_authority()
{
    std::string filename = string(ROOT_PATH) + "server.yaml";
    FILE *fp = fopen(filename.c_str(), "w");
    if (fp == NULL)
        return;

    fprintf(fp, "rules:\n");
    fprintf(fp, "  - !AUTHORITY\n");
    fprintf(fp, "    users:\n");
    fprintf(fp, "      - sharding@%%:sharding\n");
    fprintf(fp, "    provider:\n");
    fprintf(fp, "      type: ALL_PERMITTED\n");

    fclose(fp);
}

int main(int argc, char* argv[])
{
    init_log4cplus();
    log4cplus_info("************* conf-gen-utils *************");

    if(load_dtc_config(argc, argv) < 0)
        return 0;

    load_node_to_map();

    delete_all_old_yaml();

    dump_shardingsphere_conf_files();

    dump_authority();

    return 0;
}
