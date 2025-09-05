/*
* Copyright JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef PARSE_CLUSTER_CONFIG_H__
#define PARSE_CLUSTER_CONFIG_H__

#include <vector>
#include <string>
#include <map>
namespace ClusterConfig
{
#define CLUSTER_CONFIG_FILE "../conf/clusterconfig.xml"
struct ClusterNode {
	std::string name;
	std::string addr;
	bool self;
};

bool parse_cluster_config(std::string &strSelfName,
			  std::vector<ClusterNode> *result, const char *buf,
			  int len);
bool parse_cluster_config(std::vector<ClusterNode> *result);
bool save_cluster_config(std::vector<ClusterNode> *result,
			 std::string &strSelfName);
bool check_and_create(const char *filename = NULL);
bool change_node_address(std::string servername, std::string newaddress);
bool get_local_ip_by_ip_file();
bool parse_cluster_config(std::string &strSelfName,
			  std::map<std::string, std::string> &dtcClusterMap,
			  const char *buf, int len);
} // namespace ClusterConfig

#endif
