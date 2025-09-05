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
* 
*/
#ifndef CA_QUICK_FIND_H_INCLUDED_
#define CA_QUICK_FIND_H_INCLUDED_

#include "app_client_set.h"
#include "config_center_client.h"

int binary_search_header(NODE_HEADER *headers_ptr, const int low,
			 const int high, const int key);

int binary_search_node(IP_NODE *app_set_ptr, const int low, const int high,
		       const int key);

#endif
