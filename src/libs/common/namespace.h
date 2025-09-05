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
#ifndef __DTC_VERSIONED_NAMESPACE_H
#define __DTC_VERSIONED_NAMESPACE_H

#include "version.h"

#if DTC_HAS_VERSIONED_NAMESPACE && DTC_HAS_VERSIONED_NAMESPACE == 1

#ifndef DTC_VERSIONED_NAMESPACE

#define MAKE_DTC_VERSIONED_NAMESPACE_IMPL(MAJOR, MINOR, BETA)                  \
	DTC_##MAJOR##_##MINOR##_##BETA
#define MAKE_DTC_VERSIONED_NAMESPACE(MAJOR, MINOR, BETA)                       \
	MAKE_DTC_VERSIONED_NAMESPACE_IMPL(MAJOR, MINOR, BETA)
#define DTC_VERSIONED_NAMESPACE                                                \
	MAKE_DTC_VERSIONED_NAMESPACE(DTC_MAJOR_VERSION, DTC_MINOR_VERSION,     \
				     DTC_BETA_VERSION)

#endif //end DTC_VERSIONED_NAMESPACE

#define DTC_BEGIN_NAMESPACE                                                    \
	namespace DTC_VERSIONED_NAMESPACE                                      \
	{
#define DTC_END_NAMESPACE }
#define DTC_USING_NAMESPACE using namespace DTC_VERSIONED_NAMESPACE;

#else

#define DTC_BEGIN_NAMESPACE
#define DTC_END_NAMESPACE
#define DTC_USING_NAMESPACE

#endif //end DTC_HAS_VERSIONED_NAMESPACE

#endif
