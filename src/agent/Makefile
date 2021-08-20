#include ../Make.conf

VPATH := ./event ./common ./hashkit ./proto ./tools
CFLAGS+= -I./event -I./common -I./hashkit -I./proto

#for gprof only
ifeq ($(GPROF),1)
CFLAGS += -pg
LDFLAGS += -pg
endif

#建议在x86中打开
ifeq ($(USE_REGPARM),1)
CFLAGS += -DCONFIG_REGPARM=3
endif
ifeq ($(HAVE_STATS),1)
CFLAGS += -DHAVE_STATS=1
endif

ifeq ($(USE_COMPATIBLE_MODE),1)
CFLAGS += -DUSE_COMPATIBLE_MODE=1
endif

ifeq ($(HAVE_ASSERT_LOG),1)
CFLAGS += -DHAVE_ASSERT_LOG=1
endif

target = dtcagent

dtcagent_list := da da_log da_mem_pool da_util da_conn da_time da_rbtree \
				 da_signal da_array da_string da_epoll da_crc16 da_crc32 \
				 da_fnv da_chash da_hsieh da_jenkins da_ketama da_md5 da_murmur da_listener \
				 da_one_at_a_time da_msg da_client da_server da_stats da_request da_response \
				 da_buf da_dtcparse da_conf da_core da_top_percentile
dtcagent_objs := $(patsubst %,%.o,$(dtcagent_list))
dtcagent_libs := -lpthread -lrt -lmxml

target_install = dtcagent

install_dir = .
include ../Make.rules
