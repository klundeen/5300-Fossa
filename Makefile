CXX      ?= g++
CC       := $(CXX) # Ensures that linking is done with c++ libs
CXXFLAGS  = -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O2 -std=c++11 -I/usr/local/db6/include
LDFLAGS  += -L/usr/local/db6/lib
LDLIBS    = -ldb_cxx -lsqlparser

.PHONY: all
all: sql5300

# make will automatically assumes x.cpp -> x.o and x.o -> x
# when x needs more then just x.cpp add the .o files here
sql5300: sql5300.o

.PHONY: clean
clean:
	$(RM) sql5300 *.o
