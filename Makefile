CXX      = g++
CXXFLAGS = -Wall -Wextra -O2 -std=c++17 -pthread

all: smserver smspooler smclient

backend: smserver smspooler

smserver: smserver.cpp smcommon.cpp smcommon.h smutil.h
	$(CXX) $(CXXFLAGS) -o $@ smserver.cpp smcommon.cpp

smspooler: smspooler.cpp smcommon.cpp smcommon.h smutil.h
	$(CXX) $(CXXFLAGS) -o $@ smspooler.cpp smcommon.cpp

# Person 2's client (smclient.cpp). If it is plain C, use instead:
#   gcc -Wall -O2 -o smclient smclient.c   (+ compile smcommon.cpp separately)
smclient: smclient.cpp smcommon.cpp smcommon.h smutil.h
	$(CXX) $(CXXFLAGS) -o $@ smclient.cpp smcommon.cpp

clean:
	rm -f smserver smspooler smclient
	rm -rf mailboxes

.PHONY: all backend clean
