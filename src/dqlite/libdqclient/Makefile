TARGET_LIB = libdqclient
TARGET_SRCS = dqclient.c geolock.c glm_dqlite.c

TEST_DQCLIENT = test_client
TEST_DQCLIENT_SRCS = test_client.c

STRESS_TEST = stress_test
STRESS_TEST_SRCS = stress_test.c

TEST_GEOLOCK = test_geolock
TEST_GEOLOCK_SRCS = test_geolock.c

LIST_GEOLOCK = list_geolock
LIST_GEOLOCK_SRCS = list_geolock.c

ifndef NEDGE_NDEBUG
ASAN_CPPFLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common
ASAN_LDFLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common -lasan
else
ASAN_CPPFLAGS=-DUSE_JE_MALLOC -O2
ASAN_LDFLAGS=-ljemalloc -fno-omit-frame-pointer
endif

CFLAGS += -fPIC -pthread -std=gnu99 -Wall -Wstrict-aliasing=0 -Wendif-labels \
 -Wbad-function-cast -Wcast-align -Wextra \
 -Wno-missing-field-initializers -Wno-unused-variable -Wno-unused-parameter \
 -Wno-cast-align -Wno-unused-function -D_GNU_SOURCE \
 -O2 -g -I. -I../../ccow/include -I../../ccow/src/include \
 -I$(NEDGE_HOME)/include/dqlite -I$(NEDGE_HOME)/include/json-c \
 -I./include -I. -L. $(ASAN_CPPFLAGS)

LDFLAGS += $(ASAN_LDFLAGS) -L $(NEDGE_HOME)/lib -lpthread -lccowutil -lccowfsio -ldqlite

STRESS_TEST_LDFLAGS = -lpthread

TARGET_OBJS = $(TARGET_SRCS:.c=.o)
TEST_DQCLIENT_OBJS = $(TEST_DQCLIENT_SRCS:.c=.o)
STRESS_TEST_OBJS = $(STRESS_TEST_SRCS:.c=.o)
TEST_GEOLOCK_OBJS = $(TEST_GEOLOCK_SRCS:.c=.o)
LIST_GEOLOCK_OBJS = $(LIST_GEOLOCK_SRCS:.c=.o)

all: $(TARGET_LIB).a $(TARGET_LIB).so $(TEST_DQCLIENT) $(STRESS_TEST) $(TEST_GEOLOCK) $(LIST_GEOLOCK)

$(TARGET_LIB).a: $(TARGET_OBJS)
	ar rcs $@ $^

$(TARGET_LIB).so: $(TARGET_OBJS)
	gcc -shared -o $@ $^ -L $(NEDGE_HOME)/lib -ldqlite -lccowutil

$(TEST_DQCLIENT): $(TEST_DQCLIENT_OBJS) $(TARGET_OBJS) $(TARGET_LIB).a
	gcc -o $@ $^  $(LDFLAGS)

$(STRESS_TEST): $(STRESS_TEST_OBJS) $(TARGET_LIB).a
	gcc -o $@ $^  $(LDFLAGS) $(STRESS_TEST_LDFLAGS)

$(TEST_GEOLOCK): $(TEST_GEOLOCK_OBJS) $(TARGET_LIB).a
	gcc -o $@ $^  $(LDFLAGS)

$(LIST_GEOLOCK): $(LIST_GEOLOCK_OBJS) $(TARGET_LIB).a
	gcc -o $@ $^  $(LDFLAGS)

clean:
	rm -f $(STRESS_TEST) $(TEST_DQCLIENT) $(TARGET_LIB).a $(TARGET_LIB).so *.o
