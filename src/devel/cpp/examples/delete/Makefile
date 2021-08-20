LIB = ../../libdtc.so

OUTPUT = delete

SOURCES =  deletetest.cpp 
								   		

OBJECTS= deletetest.o
	CPPFLAGS = -g -O -Wall -fPIC -DMACROFOR64BIT -Wl,--no-undefined -Xlinker -zmuldefs
	CFLAGS = $(CPPFLAGS)

all:$(OUTPUT)

.SUFFIXES: .o .cpp
	.cpp.o:
		$(CXX) $(CPPFLAGS) $(INCLUDE) -c $(filter %.cpp, $^) 

.o:
		$(CXX) $(CPPFLAGS) $(INCLUDE) -o $@ $^ 

$(OUTPUT): $(OBJECTS)
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

