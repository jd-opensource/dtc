include ../Make.conf

TARGETEXE = rocksdb_connector

CC = g++

.SUFFIXES: .cc .h

FLAGS = -g -std=c++11 -DROCKSDB_COMPILER=1 -DOMN_PLATFORM_UNIX $(Optimize) -Wl,--no-undefined -Xlinker -zmuldefs

INCDIRS = -I.. -I../libs/common -I../daemons -I../core -I../libs/stat -I../libs/rocksdb/include -I../libs/rocksdb/logging $(LOG4CPLUS_INC)  $(YAML_CPP_INC)

LIBDIR = -L../libs/common  -L../core -L../libs/rocksdb/lib

OBJDIR = .

CREATEOBJDIR = $(shell if [ -d $(OBJDIR) ]; then echo ""; else mkdir $(OBJDIR); fi)

OUTPUTDIR = .

LIBS = -lnsl -lpthread -lstdc++ -ldl -lz -lrocksdb -L../bin 

DLIBS = ../core/libdtcd.a \
		../libs/common/libcommon.a \
		../libs/stat/libstat.a \
		../daemons/libdaemons.a \
		$(LOG4CPLUS_LIB) \
		$(YAML_CPP_LIB) \
#
# objects and targets
#
OBJECTS = $(wildcard *.cc)
OBJECTS:= $(filter-out comm_main.cc db_process_gauss.cc gauss_db_conn.cc, $(OBJECTS))
OBJECTS:= $(patsubst %.cc,$(OBJDIR)/%.o,$(OBJECTS))

#
# Rules for normal comile and link 
#
all:: $(TARGETEXE) 

$(TARGETEXE): $(OBJECTS)
	$(CC) -rdynamic -Wl,--no-as-needed $(FLAGS) -o $(OBJDIR)/$(TARGETEXE) $^  $(LIBDIR) $(DLIBS) $(LIBS) $(DLIBS) 
$(OBJECTS): $(OBJDIR)/%.o: %.cc
	$(CREATEOBJDIR)
	$(CC) -c -static  $(FLAGS) $< $(INCDIRS) -o $@

clean:
	rm -rf $(OBJDIR)/*.o $(OBJDIR)/$(TARGETEXE)
	rm -rf ./tester/rocksdata/*
