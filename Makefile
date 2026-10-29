CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -std=gnu11 -pthread

all: smserver smspooler smclient

backend: smserver smspooler

smserver: smserver.c smcommon.c smcommon.h smutil.h
	$(CC) $(CFLAGS) -o $@ smserver.c smcommon.c

smspooler: smspooler.c smcommon.c smcommon.h smutil.h
	$(CC) $(CFLAGS) -o $@ smspooler.c smcommon.c

smclient: smclient.c smcommon.c smcommon.h smutil.h
	$(CC) $(CFLAGS) -o $@ smclient.c smcommon.c

clean:
	rm -f smserver smspooler smclient
	rm -rf mailboxes

.PHONY: all backend clean
