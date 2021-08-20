LIB_PATH = ../..

include ../../Make.conf

GIT_VERSION=$(shell git log . | head -n 4 | grep "commit" | cut -d " " -f 2 | cut -c 1-7)
SVN_REVISION = $(shell test -d .svn && (svn info | grep "Last Changed Rev" | cut -d " " -f 4))
ifeq "$(SVN_REVISION)a" "a"
	SVN_REVISION = "(unknown)"
endif

SRC_FOLDER := $(shell find ../../libs/common -maxdepth 1 -type d)
VPATH = ../../libs/common ../../configcenter/ca_api/ $(basename $(patsubst ./%, %/, $(SRC_FOLDER)))
################compile#############
target = libdtc.a container_api.pic.o version.pic.o somain.pic.o libdtc.so libdtc.pic.a 

# CLIENTAPI macro use for scope test only
CFLAGS += -DCLIENTAPI
CFLAGS += -D_GLIBCXX_USE_CXX11_ABI=0 -std=gnu++11
CFLAGS += -pthread -I../../libs/common -I../../stat -I../../configcenter/ca_api/ $(LOG4CPLUS_INC) $(ZINC) $(YAML_CPP_INC)
LIBS = ${Z_LIB} -ldl -lpthread  $(LOG4CPLUS_LIB) $(CA_API_LIB) -L../../libs/common -lcommon $(YAML_CPP_LIB)

filelist := dtcreq value dtcsvr dtcpool dtcwrap dtcwrapp dtcqossvr \
	poller timer_list key_list table_def log_client mem_check \
	md5 section decode encode field_api packet_base \
	packet_client task_base task_const socket_addr udppool compress buffer thread
#filelist := dtcreq

#lidttc.so: container_api.pic.o version.pic.o

# for auto ln -sf
libdtc_objs := $(patsubst %,%.o,$(filelist))
libdtc.so: LDFLAGS += -Wl,--version-script,dtcapi.lst -e _so_start container_api.pic.o version.pic.o somain.pic.o
libdtc_filename := libdtc-gcc-$(GCCVER)-r$(GIT_VERSION).so
libdtc_soname := libdtc.so.1

###############install##############
target_install = libdtc.a libdtc.pic.a libdtc.so dtcapi.h 
install_dir = ../../../bin
%.a: install_dir = ../../../lib
%.h: install_dir = ../../../include

include ../../Make.rules

