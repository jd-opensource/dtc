OBJECTS = app_client_set.o app_shm_manager.o ca_quick_find.o

C_ARGS = -g -Wall -fPIC
SHARED = -shared
STATIC = ar
LD_ARGS = rcs

CXX = g++
CC = gcc

DY_BINARY = libapp-client.so
ST_BINARY = libapp-client.a
CLIENT_TOOL = client-tool

all: $(ST_BINARY) $(CLIENT_TOOL)

.cpp.o:
	$(CXX) $(C_ARGS) -c $^ 

.cc.o:
	$(CXX) $(C_ARGS) -c $^ 

.c.o:
	$(CC) $(C_ARGS) -c $^ 

libapp-client.a: $(OBJECTS)
	$(STATIC) $(LD_ARGS) $@ $^

	@export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:. ; $(CC) $(C_ARGS) ca_dump_shm.c -o $(CLIENT_TOOL) -g -L./ -lapp-client
	
.PHONY: clean
clean:
	-rm -f *.o $(DY_BINARY) ${ST_BINARY} $(CLIENT_TOOL)
