LIB = ../../libdtc.so

OUTPUT1 = selecttest

SOURCES =  selecttest.cpp 
								   		
OUTPUT2 = TestServerPool

SOURCES = TestServerPool.cpp

OBJECTS1= selecttest.o
	CPPFLAGS = -g -O -Wall -fPIC -DMACROFOR64BIT -Wl,--no-undefined -Xlinker -zmuldefs
	CFLAGS = $(CPPFLAGS)

OBJECTS2=TestServerPool.o
	CPPFLAGS = -g -O -Wall -fPIC -DMACROFOR64BIT -Wl,--no-undefined -Xlinker -zmuldefs
	CFLAGS = $(CPPFLAGS)
all:$(OUTPUT1) $(OUTPUT2) 

.SUFFIXES: .o .cpp
	.cpp.o:
		$(CXX) $(CPPFLAGS) $(INCLUDE) -c $(filter %.cpp, $^) 

.o:
		$(CXX) $(CPPFLAGS) $(INCLUDE) -o $@ $^ 

$(OUTPUT1): $(OBJECTS1)
		$(CXX) $(CFLAGS) -fPIC  -o $@ $^ $(LIB)

$(OUTPUT2): $(OBJECTS2)
		$(CXX) $(CFLAGS) -fPIC  -o $@ $^ $(LIB)

clean:
		rm -f *.o *.so *.~ *.bak
		rm -f $(OUTPUT1)
		rm -f $(OUTPUT2)

install:
		cp -f $(OUTPUT) $(INSTALL_PATH)

dep:
		mkdep -p$(VPATH) $(INCLUDE) $(SOURCES) > .depend

ifneq ($(wildcard .depend),)
	include .depend
endif

