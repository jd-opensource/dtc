LIB_PATH = ../..

include ../../Make.conf

#################compile ############
target = libstat.a stattool

CFLAGS+=-I../common -std=gnu++11 $(LOG4CPLUS_INC)
ifneq ($(findstring x86_64,$(PLATFORM)),)
BITS=64
else
BITS=32
endif

filelist := stat_manager stat_dtc stat_thread stat_dtc_def stat_client stat_attn stat_table_formater stat_manager_container_thread stat_alarm_reporter stat_tool
libstat_objs := $(filelist:%=%.o)
libstat_objs: CFLAGS += -pthread


stattool_src := stat_tool stat_table_formater stat_manager stat_attn stat_client stat_dtc_def stat_alarm_reporter
stattool_objs := $(stattool_src:%=%.o)
stattool_libs := -L../common -lcommon -ldl -lpthread $(LOG4CPLUS_LIB) $(YAML_CPP_LIB)

##################install #############
target_install = stattool
install_dir = ../../../bin

include ../../Make.rules

