SRCDIR=$(NEDGE_HOME)
TARGET_BIN = ccow-sql
TARGET_SRCS = dqlite.c
#
# Activate by exporing this env variable:
#
# export ASAN_OPTIONS=symbolize=1:abort_on_error=1:disable_core=1:alloc_dealloc_mismatch=0:detect_leaks=1
#
ifndef NEDGE_NDEBUG
ASAN_CPPFLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common -O0
ASAN_LDFLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common -lasan
else
ASAN_CPPFLAGS=-DUSE_JE_MALLOC -O2
ASAN_LDFLAGS=-ljemalloc -fno-omit-frame-pointer
endif
CFLAGS += $(ASAN_CPPFLAGS) -fPIC -pthread -std=gnu99 -Wall -Wstrict-aliasing=0 -Wendif-labels \
	  -Wpointer-arith -Wbad-function-cast -Wcast-align -Wextra \
	  -Wno-missing-field-initializers -Wno-unused-variable -Wno-unused-parameter \
	  -Wno-cast-align -Wno-unused-function -D_GNU_SOURCE \
	  -g -I. -I../ccow/include
LDFLAGS += $(ASAN_LDFLAGS) -ldqlite -L$(NEDGE_HOME)/lib -lccowutil
LXFLAGS += $(ASAN_LDFLAGS) -ldqlite -L$(NEDGE_HOME)/lib -lccowutil

TARGET_OBJS = $(TARGET_SRCS:.c=.o)

.PHONY: all
all: ${TARGET_BIN} install

$(TARGET_BIN): $(TARGET_OBJS)
	gcc -o $@ $^ $(LXFLAGS)

install: $(TARGET_BIN)
	mkdir -p $(DESTDIR)$(SRCDIR)/sbin 2> /dev/null || true
	cp -a $(TARGET_BIN) $(DESTDIR)$(SRCDIR)/sbin

uninstall:
	rm -f $(DESTDIR)$(SRCDIR)/sbin/$(TARGET_BIN)

clean:
	rm -f $(TARGET_BIN) *.o
