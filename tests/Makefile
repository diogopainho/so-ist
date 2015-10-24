SRCS=test_kos_multi_threaded.c 
#SRCS=test_kos_single_threaded.c 

OBJS=${SRCS:.c=}

INCLUDES = -I. -I$(srcdir) -I../include -I ../include 
CC = gcc
CFLAGS ?= -g -O0 -Wall -pthread 
DEFS ?= -DHAVE_CONFIG_H -DUSE_PTHREADS
LIBKOS =  ../kos/libkos.a

COMPILE = $(CC) $(DEFS) $(INCLUDES) $(CFLAGS)

all: $(OBJS)

$(OBJS): $(SRCS)
	$(COMPILE) -o $@ $<  $(LIBKOS)

clean: 
	rm -f $(OBJS)


# Tell versions [3.59,3.63) of GNU make to not export all variables.
# Otherwise a system limit (for SysV at least) may be exceeded.
.NOEXPORT:
