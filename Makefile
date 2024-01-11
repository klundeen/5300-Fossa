sql5300: sql5300.o
	g++ -L/usr/local/db6/lib -L./lib -o $@ $< -ldb_cxx -lsqlparser

sql5300.o : sql5300.cpp
	g++ -I/usr/local/db6/include -I./include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O2 -std=c++11 -c -o $@ $<

clean : 
	rm -f sql5300{.o,}
