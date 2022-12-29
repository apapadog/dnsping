CC=g++
RM=rm -f
CPPFLAGS=-I/usr/include/mysql -I/usr/include/mysql++ -I/usr/local/include/mysql++ -g
LIBS=-lldns -lmysqlclient -lmysqlpp

SOURCES=$(wildcard *.cpp)
TARGETS=dnsping querydb

all: $(TARGETS)

dnsping: dnsping.cpp
	$(CC) $(CPPFLAGS) $^ -o $@ $(LIBS)

querydb: querydb.cpp
	$(CC) $(CPPFLAGS) $^ -o $@ $(LIBS)

clean:
	$(RM) *.o $(TARGETS)


