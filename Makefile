CXX      ?= g++
CC       := $(CXX) # Ensures that linking is done with c++ libs
CPPFLAGS  = -I/usr/local/db6/include -Iinclude
CXXFLAGS  = -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O2 -std=c++17
LDFLAGS  += -L/usr/local/db6/lib
LDLIBS    = -ldb_cxx -lsqlparser

SRC_DIR := src

.PHONY: all
all: sql5300

# Must run `make clean` first if source has not changed.
.PHONY: debug
debug: CXXFLAGS = -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -g -std=c++17
debug: all

# make will automatically assumes x.cpp -> x.o and x.o -> x
# when x needs more then just x.cpp add the .o files here
sql5300: sql5300.o Execute.o
sql5300: heap_storage.o test_heap_storage.o

%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

.PHONY: clean
clean:
	$(RM) sql5300 *.o
