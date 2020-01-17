TARGET_LIB = libdqclient
TARGET_SRCS = dqclient.c

TEST_DQCLIENT = test_client
TEST_DQCLIENT_SRCS = test_client.c

ifndef NEDGE_NDEBUG
ASAN_CPPFLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common
ASAN_LDFLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common -lasan
endif

CFLAGS += -fPIC -pthread -std=gnu99 -Wall -Wstrict-aliasing=0 -Wendif-labels \
 -Wbad-function-cast -Wcast-align -Wextra \
 -Wno-missing-field-initializers -Wno-unused-variable -Wno-unused-parameter \
 -Wno-cast-align -Wno-unused-function -D_GNU_SOURCE \
 -O2 -g -I. -I../../ccow/include -I../../ccow/src/include \
 -I/opt/nedge/include/dqlite -I./include -I. -L. $(ASAN_CPPFLAGS)

LDFLAGS += $(ASAN_LDFLAGS) -L /opt/nedge/lib -ldqlite

TARGET_OBJS = $(TARGET_SRCS:.c=.o)
TEST_DQCLIENT_OBJS = $(TEST_DQCLIENT_SRCS:.c=.o)

all: $(TARGET_LIB).a test_client

$(TARGET_LIB).a: $(TARGET_OBJS)
	ar rcs $@ $^

$(TEST_DQCLIENT): $(TEST_DQCLIENT_OBJS) $(TARGET_OBJS) $(TARGET_LIB).a
	gcc -o $@ $^  $(LDFLAGS)

clean:
	rm -f $(TEST_DQCLIENT) $(TARGET_LIB).a *.o
