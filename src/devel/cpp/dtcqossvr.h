#ifndef __CHC_CLI_SERVERS_H
#define __CHC_CLI_SERVERS_H

#include "dtcint.h"

extern "C"
{
#include "app_client_set.h"
}

#define DEFAULT_ROUTE_EXPIRE_TIME 120
#define DEFAULT_MAX_REMOVE_THRESHOLD_TIME 1800
#define DEFAULT_REMOVE_ERROR_COUNT 3
#define DEFAULT_ROUTE_INTERVAL_TIME 5
#define DEFAULT_SERVER_POS 0

namespace DTC 
{
	class DTCQosServer
	{
	public:
		friend class DTCServers;

	private:
		int m_status;
		int m_weight;
		/* 需要被摘除的时间 */
		uint64_t m_remove_time_stamp;
		/* 上次被移除的时间点 */
		uint64_t m_last_remove_time; 
		/* 最大指数阀值 */
		uint64_t m_max_time_stamp;
		Server *m_server;

    private:
		DTCQosServer(const DTCQosServer& qosServer);
		DTCQosServer& operator=(const DTCQosServer& qosServer);
	public:
		DTCQosServer();
		~DTCQosServer(void);

	public:
		int get_status()
		{
			return this->m_status;
		}
		void set_status(int iFlag)
		{
			this->m_status = iFlag;
		}

		int get_weight()
		{
			return this->m_weight;
		}
		void set_weight(int iFlag)
		{
			this->m_weight = iFlag;
		}

		uint64_t get_remove_time_stamp()
		{
			return this->m_remove_time_stamp;
		}	
		void set_remove_time_stamp(uint64_t iFlag)
		{
			this->m_remove_time_stamp = iFlag;
		}

		uint64_t get_last_remove_time()
		{
			return this->m_last_remove_time;
		}	
		void set_last_remove_time(uint64_t iFlag)
		{
			this->m_last_remove_time = iFlag;
		}

		uint64_t get_max_time_stamp()
		{
			return this->m_max_time_stamp;
		}	
		void set_max_time_stamp(uint64_t iFlag)
		{
			this->m_max_time_stamp = iFlag;
		}

		Server* get_dtc_server()
		{
			return this->m_server;
		}

		void set_dtc_server(Server* ser)
		{
			this->m_server = ser;
		}

		void reset_dtc_server()
		{
			this->m_server = NULL;
		}

		void create_dtc_server()
		{
			this->m_server = new Server();
		}
	};

};

#endif 