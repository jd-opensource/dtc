include ../Make.conf

target = libdaemons.a faultlogger

CFLAGS+=-I../libs/common -I../libs/stat -std=gnu++11 -fPIC $(LOG4CPLUS_INC)  $(YAML_CPP_INC)
VPATH := ../libs/stat
filelist := daemons base fork helper daemon_listener main run stattool unit logger fault gdb gdb_srv stat_alarm_reporter
libdaemons_objs := $(filelist:%=%.o) $(LOG4CPLUS_LIB) $(YAML_CPP_LIB) 

faultlogger: LDFLAGS += -shared -e _so_start

faultlogger_objs = fault.pic.o

fault.pic.o fault.o: CFLAGS += -O0

target_install = faultlogger

install_dir = ../../bin

include ../Make.rules

