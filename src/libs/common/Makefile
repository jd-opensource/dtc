LIB_PATH = ../..

include ../../Make.conf

target = libcommon.a libcommon_nopic.a
CFLAGS+=-I../../devel/cpp -I. -I../../daemons -I../stat $(LOG4CPLUS_INC) $(ZINC) -std=gnu++11 $(YAML_CPP_INC)
LIBS = $(Z_LIB)  $(LOG4CPLUS_LIB) $(YAML_CPP_LIB) 
SRC_FOLDER := $(shell find . -maxdepth 1 -type d)
VPATH = ../../daemons $(basename $(patsubst ./%, %/, $(SRC_FOLDER)))
GIT_VERSION=$(shell git log | head -n 4 | grep "commit" | cut -d " " -f 2 | cut -c 1-7)
ifeq "$(GIT_VERSION)a" "a"
	GIT_REVISION = "(unknown)"
endif
$(shell sed -i '11s/"[^"]*"/"$(GIT_VERSION)"/' version.h)

filelist := md5 value decode encode field section table_def \
	packet_base packet_server packet_result \
	task_const task_base task_pkey task_server task_copy task_request task_api \
	poller timer_list config version system_lock log \
	shmem mem_check dbconfig dbconfig_tdef hotbackup_table_def \
	daemon daemon_wait proc_title thread poller_base poll_thread_group thread_cpu_stat \
	buffer localip \
	key_helper bitsop socket_addr socket_bind blacklist blacklist_unit \
	multi_request task_multi_unit \
	connector_client data_connector_ask_chain connector_group \
	listener listener_pool decoder_base \
	client_sync client_async client_unit client_dgram  \
	plugin_request plugin_mgr plugin_dgram plugin_sync \
	plugin_worker plugin_unit plugin_listener_pool \
	plugin_global plugin_decoder plugin_timer dtc_global\
	compress client_resource_pool \
	new_hash remote_dtc_ask_answer_chain consistent_hash_selector chash\
	file_backed_key_set markup_stl parse_cluster_config key_route_ask_chain \
	agent_client job_entrance_ask_chain agent_listener agent_listen_pool agent_unit agent_receiver agent_sender \
	agent_multi_request plugin_proxy_mgr plugin_proxy_listener_pool fork hotback_task table_def_manager

libcommon_objs := $(filelist:%=%_pic.o)
%_pic.o: CFLAGS  += -pthread -fPIC 

libcommon_nopic_objs := $(filelist:%=%.o)
%.o: CFLAGS  += -pthread

include ../../Make.rules

