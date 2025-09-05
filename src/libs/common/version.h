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

#ifndef __DTC_VERSION_H
#define __DTC_VERSION_H

#define DTC_MAJOR_VERSION 4
#define DTC_MINOR_VERSION 7
#define DTC_BETA_VERSION 1

/*major.minor.beta*/
#define DTC_VERSION "4.7.1"
/* the following show line should be line 11 as line number is used in Makefile
 */
#define DTC_GIT_VERSION "7b21244"
#define DTC_VERSION_DETAIL DTC_VERSION "." DTC_GIT_VERSION

extern const char compdatestr[];
extern const char comptimestr[];
extern const char version[];
extern const char version_detail[];
extern long compile_time;

#endif
