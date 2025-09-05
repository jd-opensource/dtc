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
#ifndef __DTC_UTILS__
#define __DTC_UTILS__
#include <vector>
#include <string>

#include <sys/socket.h>
#include <iostream>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>

/*This file contains dtc utility functions*/
namespace dtc
{
namespace utils
{
/*************************************************
		Get local IP tomchen
		**************************************************/
inline std::string get_local_ip()
{
	int iClientSockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (iClientSockfd < 0)
		return "0.0.0.0";

	struct sockaddr_in stINETAddr;
	stINETAddr.sin_addr.s_addr = inet_addr("192.168.0.1");
	stINETAddr.sin_family = AF_INET;
	stINETAddr.sin_port = htons(8888);

	int iCurrentFlag = fcntl(iClientSockfd, F_GETFL, 0);
	fcntl(iClientSockfd, F_SETFL, iCurrentFlag | FNDELAY);

	if (connect(iClientSockfd, (struct sockaddr *)&stINETAddr,
		    sizeof(sockaddr)) != 0) {
		close(iClientSockfd);
		return "0.0.0.0";
	}

	struct sockaddr_in stINETAddrLocal;
	socklen_t iAddrLenLocal = sizeof(stINETAddrLocal);
	getsockname(iClientSockfd, (struct sockaddr *)&stINETAddrLocal,
		    &iAddrLenLocal);

	close(iClientSockfd);

	return inet_ntoa(stINETAddrLocal.sin_addr);
}
/*************************************************
		Split string strOri using _Ch as delimiter, result stored in theVec
		**************************************************/
inline void split_str(std::string strOri, char _Ch,
		      std::vector<std::string> &theVec)
{
	std::string::size_type nLastPos = 0;
	std::string strSub;
	std::string::size_type iIndex = strOri.find(_Ch);

	if (std::string::npos == iIndex) {
		theVec.push_back(strOri);
		return;
	}

	while (std::string::npos != iIndex) {
		strSub = strOri.substr(nLastPos, iIndex - nLastPos);
		nLastPos = iIndex + 1;
		iIndex = strOri.find(_Ch, nLastPos);
		theVec.push_back(strSub);
	}

	if (nLastPos != 0) {
		strSub = strOri.substr(nLastPos, strOri.length() - nLastPos);
		theVec.push_back(strSub);
	}
}

inline int get_bid()
{
	char buf[1024];
	getcwd(buf, sizeof(buf));
	std::vector<std::string> pathVec;
	dtc::utils::split_str(std::string(buf), '/', pathVec);
	if (pathVec.size() < 5) {
		return 0;
	}
	std::string strAccessKey = pathVec[4];
	std::string strModuleId = strAccessKey.substr(0, 8);
	return atoi(strModuleId.c_str());
}
} // namespace utils
} // namespace dtc

#endif