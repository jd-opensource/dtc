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
#include <errno.h>
#include <stdio.h>
#include "config/config.h"
#include <set>

#include "config/parse_cluster_config.h"
#include "algorithm/markup_stl.h"
#include "log/log.h"
#include "socket/socket_addr.h"
#include <fstream>

using namespace std;

extern int errno;
extern DTCConfig *g_dtc_config;

namespace ClusterConfig
{
bool parse_cluster_config(std::string &strSelfName,
			  std::vector<ClusterNode> *result, const char *buf,
			  int len)
{
	log4cplus_debug("%.*s", len, buf);
	// Configuration does not allow nodes with same servername to appear, this set is used to check duplicate servername

	std::set<std::string> filter;

	pair<set<std::string>::iterator, bool> ret;

	string xmldoc(buf, len);

	MarkupSTL xml;

	if (!xml.set_doc(xmldoc.c_str())) {
		log4cplus_error("parse config file error");

		return false;
	}

	xml.reset_main_pos();

	if (!xml.find_elem("serverlist")) {
		log4cplus_error("no serverlist info");
		return -1;
	}

	strSelfName = xml.get_attrib("selfname");

	log4cplus_debug("strSelfName is %s", strSelfName.c_str());

	while (xml.find_child_elem("server")) {
		if (xml.into_elem()) {
			struct ClusterNode node;
			node.name = xml.get_attrib("name");
			node.addr = xml.get_attrib("address");
			if (node.name.empty() || node.addr.empty()) {
				log4cplus_error("name:%s addr:%s may error\n",
						node.name.c_str(),
						node.addr.c_str());
				return false;
			}

			if (strSelfName == node.name) {
				node.self = true;
			} else {
				node.self = false;
			}

			ret = filter.insert(node.addr);
			if (ret.second == false) {
				log4cplus_error(
					"server address duplicate. name:%s addr:%s \n",
					node.name.c_str(), node.addr.c_str());
				return false;
			}
			result->push_back(node);
			xml.out_of_elem();
		}
	}
	if (result->empty())
		log4cplus_info(
			"ClusterConfig is empty,dtc runing in normal mode");
	return true;
}

// Read configuration file
bool parse_cluster_config(std::vector<ClusterNode> *result)
{
	bool bResult = false;
	FILE *fp = fopen(CLUSTER_CONFIG_FILE, "rb");
	if (fp == NULL) {
		log4cplus_error("load %s failed:%m", CLUSTER_CONFIG_FILE);
		return false;
	}

	// Determine file length
	fseek(fp, 0L, SEEK_END);
	int nFileLen = ftell(fp);
	fseek(fp, 0L, SEEK_SET);

	// Load string
	allocator<char> mem;
	std::string strSelfName;
	allocator<char>::pointer pBuffer = mem.allocate(nFileLen + 1, NULL);
	if (fread(pBuffer, nFileLen, 1, fp) == 1) {
		pBuffer[nFileLen] = '\0';
		bResult = parse_cluster_config(strSelfName, result, pBuffer,
					       nFileLen);
	}
	fclose(fp);
	mem.deallocate(pBuffer, 1);
	return bResult;
}

bool parse_cluster_config(std::string &strSelfName,
			  std::map<std::string, std::string> &dtcClusterMap,
			  const char *buf, int len)
{
	log4cplus_debug("%.*s", len, buf);

	string xmldoc(buf, len);
	MarkupSTL xml;
	if (!xml.set_doc(xmldoc.c_str())) {
		log4cplus_error("parse config file error");
		return false;
	}
	xml.reset_main_pos();

	if (!xml.find_elem("serverlist")) {
		log4cplus_error("no serverlist info");
		return -1;
	}
	strSelfName = xml.get_attrib("selfname");
	log4cplus_debug("strSelfName is %s", strSelfName.c_str());

	while (xml.find_child_elem("server")) {
		if (xml.into_elem()) {
			std::string name = xml.get_attrib("name");
			std::string addr = xml.get_attrib("address");
			if (name.empty() || addr.empty()) {
				log4cplus_error("name:%s addr:%s may error\n",
						name.c_str(), addr.c_str());
				return false;
			}

			std::pair<std::map<std::string, std::string>::iterator,
				  bool>
				ret;
			ret = dtcClusterMap.insert(
				std::pair<std::string, std::string>(name,
								    addr));
			if (ret.second) {
				log4cplus_debug(
					"insert dtcClusterMap success: name[%s]addr[%s]",
					name.c_str(), addr.c_str());
			} else {
				log4cplus_error(
					"insert dtcClusterMap fail,maybe dtc name duplicated: name[%s]addr[%s]",
					name.c_str(), addr.c_str());
			}

			xml.out_of_elem();
		}
	}
	return true;
}

// Save vector to configuration file
bool save_cluster_config(std::vector<ClusterNode> *result,
			 std::string &strSelfName)
{
	MarkupSTL xml;
	xml.set_doc("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
	xml.add_elem("serverlist");
	xml.add_attrib("selfname", strSelfName.c_str());
	xml.into_elem();

	std::vector<ClusterNode>::iterator it;
	for (it = result->begin(); it < result->end(); it++) {
		xml.add_elem("server");
		xml.add_attrib("name", it->name.c_str());
		xml.add_attrib("address", it->addr.c_str());
	}
	return xml.Save(CLUSTER_CONFIG_FILE);
}

// Check if configuration file exists, if not then generate configuration file
bool check_and_create(const char *filename)
{
	if (filename == NULL)
		filename = CLUSTER_CONFIG_FILE;

	if (access(filename, F_OK)) {
		// Configuration file does not exist, create configuration file
		if (errno == ENOENT) {
			log4cplus_info("%s didn't exist,create it.", filename);
			MarkupSTL xml;
			xml.set_doc(
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
			xml.add_elem("serverlist");
			xml.add_attrib("selfname", "");
			xml.into_elem();
			return xml.Save(filename);
		}
		log4cplus_error("access %s error:%m\n", filename);
		return false;
	} else
		return true;
}

bool change_node_address(std::string servername, std::string newaddress)
{
	MarkupSTL xml;
	if (!xml.Load(CLUSTER_CONFIG_FILE)) {
		log4cplus_error("load config file: %s error",
				CLUSTER_CONFIG_FILE);
		return false;
	}
	xml.reset_main_pos();
	while (xml.find_child_elem("server")) {
		if (!xml.into_elem()) {
			log4cplus_error("xml.into_elem() error");
			return false;
		}

		string name = xml.get_attrib("name");
		string address = xml.get_attrib("address");
		if (address == newaddress) {
			log4cplus_error(
				"new address:%s is exist in Cluster Config",
				newaddress.c_str());
			return false;
		}

		if (!xml.out_of_elem()) {
			log4cplus_error("xml.OutElem() error");
			return false;
		}
	}

	xml.reset_main_pos();
	while (xml.find_child_elem("server")) {
		if (!xml.into_elem()) {
			log4cplus_error("xml.into_elem() error");
			return false;
		}

		string name = xml.get_attrib("name");
		if (name != servername) {
			if (!xml.out_of_elem()) {
				log4cplus_error("xml.OutElem() error");
				return false;
			}
			continue;
		}

		string oldaddress = xml.get_attrib("address");
		if (oldaddress == newaddress) {
			log4cplus_error(
				"server name:%s ip address:%s not change",
				servername.c_str(), newaddress.c_str());
			return false;
		}

		if (!xml.set_attrib("address", newaddress.c_str())) {
			log4cplus_error(
				"change address error.server name: %s old address: %s new address: %s",
				servername.c_str(), oldaddress.c_str(),
				newaddress.c_str());
			return false;
		}
		return xml.Save(CLUSTER_CONFIG_FILE);
	}
	log4cplus_error("servername: \"%s\" not found", servername.c_str());
	return false;
}
} // namespace ClusterConfig
