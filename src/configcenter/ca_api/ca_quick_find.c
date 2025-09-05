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
#include "ca_quick_find.h"

//Binary search
int binary_search_header(NODE_HEADER *headers_ptr, const int low,
			 const int high, const int key)
{
	if (low > high) {
		return -1;
	}

	int mid_index = (low + high) / 2;

	if (key == headers_ptr[mid_index].bid) {
		while (1) {
			--mid_index;
			if (mid_index >= 0 &&
			    key == headers_ptr[mid_index].bid) {
				continue;
			} else {
				mid_index += 1;
				break;
			}
		}
		return mid_index;
	} else if (key > headers_ptr[mid_index].bid) {
		return binary_search_header(headers_ptr, mid_index + 1, high,
					    key);
	} else {
		return binary_search_header(headers_ptr, low, mid_index - 1,
					    key);
	}
}

int binary_search_node(IP_NODE *app_set_ptr, const int low, const int high,
		       const int key)
{
	if (low > high) {
		return -1;
	}

	int mid_index = (low + high) / 2;

	if (key == app_set_ptr[mid_index].bid) {
		while (1) {
			--mid_index;
			if (mid_index >= 0 &&
			    key == app_set_ptr[mid_index].bid) {
				continue;
			} else {
				mid_index += 1;
				break;
			}
		}
		return mid_index;
	} else if (key > app_set_ptr[mid_index].bid) {
		return binary_search_node(app_set_ptr, mid_index + 1, high,
					  key);
	} else {
		return binary_search_node(app_set_ptr, low, mid_index - 1, key);
	}
}
