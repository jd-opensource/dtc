include ../Make.conf

SRC_FOLDER := $(shell find . -maxdepth 1 -type d)
VPATH :=../libs/stat $(basename $(patsubst ./%, %/, $(SRC_FOLDER)))

####################compile#################
CFLAGS += -D_GLIBCXX_USE_CXX11_ABI=0 -std=gnu++11
CFLAGS += -I./ -I../libs/common -I../devel/cpp -I../libs/stat -I../daemons -I../connector $(LOG4CPLUS_INC)  $(YAML_CPP_INC)
CFLAGS += $(basename $(patsubst ./%, -I%, $(SRC_FOLDER)))

LIBPATH := -L. -L../libs/common -L../daemons -L../libs/stat -L../devel $(LOG4CPLUS_LIB) $(YAML_CPP_LIB)
DB%.o tmp.DB%.o:CFLAGS += $(MYSQLINC)
ifneq ($(findstring x86_64,$(PLATFORM)),)
BITS=64
else
BITS=32
endif

LIBDTCAPI := -L../devel  -lpthread -Wl,-rpath,\$$ORIGIN/../lib/ -Wl,-rpath,\$$ORIGIN  -Wl,-rpath,\$$ORIGIN/../devel/  -Wl,-rpath,\$$ORIGIN/../ -z origin

target = libdtcd.a dtcd
target_external = ../devel/cpp/libdtc.a ../libs/stat/libstat.a ../libs/common/libcommon.a 

$(filterout libdtcd.a,$(target)): libdtcd.a;

filelist := feature hash ng_info node_set node_index barrier_ask_answer_chain buffer_bypass_answer_chain buffer_bypass_ask_chain buffer_pond\
		pt_malloc sys_malloc raw_data raw_data_process buffer_process_answer_chain buffer_process_ask_chain buffer_flush\
		job_procedure empty_filter black_hole_ask_chain logger task_pendlist lru_bit hb_log\
		hb_feature container_dtcd col_expand t_tree tree_data\
		tree_data_process expire_time main_supply main hot_backup_ask_chain system_command_ask_chain 
libdtcd_objs:= $(sort $(filelist:%=%.o))

#dtcd
dtcd: CFLAGS += -export-dynamic
dtcd: LDFLAGS += -Wl,--version-script,dtcd.export.lst
dtcd_objs:= main.o system_command_ask_chain.o stat_client.o expire_time.o hot_backup_ask_chain.o 
dtcd_libs:= -lstat -ldaemons  -ldtcd -lcommon -lpthread -ldl $(Z_LIB) -rdynamic

#####################install############
target_install = dtcd
install_dir = ../../bin

include ../Make.rules
