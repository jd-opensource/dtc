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
#ifndef __DTC_CONTAINER_HEADER__
#define __DTC_CONTAINER_HEADER__

class DTCTableDefinition;
class DTCJobOperation;
class NCRequest;
class NCResultInternal;
union DTCValue;

#if GCC_MAJOR >= 4
#pragma GCC visibility push(default)
#endif
class IInternalService {
    public:
	virtual ~IInternalService(void)
	{
	}

	virtual const char *query_version_string(void) = 0;
	virtual const char *query_service_type(void) = 0;
	virtual const char *query_instance_name(void) = 0;
};

class IDTCTaskExecutor {
    public:
	virtual ~IDTCTaskExecutor(void)
	{
	}

	virtual NCResultInternal *task_execute(NCRequest &,
					       const DTCValue *) = 0;
};

class IDTCService : public IInternalService {
    public:
	virtual ~IDTCService(void)
	{
	}

	virtual DTCTableDefinition *query_table_definition(void) = 0;
	virtual DTCTableDefinition *query_admin_table_definition(void) = 0;
	virtual IDTCTaskExecutor *query_task_executor(void) = 0;
	virtual int match_listening_ports(const char *, const char * = 0) = 0;
};
#if GCC_MAJOR >= 4
#pragma GCC visibility pop
#endif

IInternalService *query_internal_service(const char *name,
					 const char *instance);

#endif
