CC            ?= cc
COMMON_CFLAGS := -Wall -Wextra -std=c11 -Iinclude
CFLAGS        := $(COMMON_CFLAGS) -O2 -pthread
LDFLAGS := -pthread

TSAN_CFLAGS  := $(COMMON_CFLAGS) -O1 -g -pthread -fsanitize=thread
TSAN_LDFLAGS := $(LDFLAGS) -fsanitize=thread

BUILD      := build
TSAN_BUILD := $(BUILD)/tsan

# Modules shared by the server and the tests that exercise them directly.
PROTO  := src/protocol.c
TABLE  := src/hashtable.c
WAL    := src/wal.c
HDRS   := include/protocol.h include/hashtable.h include/wal.h

SERVER      := $(BUILD)/pulsekv
SERVER_LT   := $(BUILD)/pulsekv_lt
TEST_CLIENT := $(BUILD)/test_client
TEST_MULTI  := $(BUILD)/test_multi_client
TEST_TABLE  := $(BUILD)/test_hashtable
TEST_STRESS := $(BUILD)/test_thread_stress
TEST_WAL    := $(BUILD)/test_wal
TEST_WAL_SERVER := $(BUILD)/test_wal_server
TEST_RECOVERY := $(BUILD)/test_recovery
BENCHMARK := $(BUILD)/benchmark

TSAN_SERVER      := $(TSAN_BUILD)/pulsekv
TSAN_TEST_MULTI  := $(TSAN_BUILD)/test_multi_client
TSAN_TEST_TABLE  := $(TSAN_BUILD)/test_hashtable
TSAN_TEST_STRESS := $(TSAN_BUILD)/test_thread_stress
TSAN_TEST_WAL    := $(TSAN_BUILD)/test_wal
TSAN_TEST_WAL_SERVER := $(TSAN_BUILD)/test_wal_server
TSAN_TEST_RECOVERY := $(TSAN_BUILD)/test_recovery
TSAN_BENCHMARK := $(TSAN_BUILD)/benchmark

.PHONY: all clean tsan bench bench-lt

all: $(SERVER) $(SERVER_LT) $(TEST_CLIENT) $(TEST_MULTI) $(TEST_TABLE) \
	$(TEST_STRESS) $(TEST_WAL) $(TEST_WAL_SERVER) $(TEST_RECOVERY) \
	$(BENCHMARK)

tsan: $(TSAN_SERVER) $(TSAN_TEST_MULTI) $(TSAN_TEST_TABLE) \
	$(TSAN_TEST_STRESS) $(TSAN_TEST_WAL) $(TSAN_TEST_WAL_SERVER) \
	$(TSAN_TEST_RECOVERY) $(TSAN_BENCHMARK)

bench: all
	tests/run_benchmarks.sh $(SERVER)

bench-lt: all
	tests/run_benchmarks.sh $(SERVER_LT)

$(SERVER): src/main.c $(PROTO) $(TABLE) $(WAL) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ src/main.c $(PROTO) $(TABLE) $(WAL) $(LDFLAGS)

# Level-triggered build of the same source. Step 2 is a progression from LT to
# ET, so keep both buildable and run the concurrency test against each.
$(SERVER_LT): src/main.c $(PROTO) $(TABLE) $(WAL) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -DPULSEKV_LEVEL_TRIGGERED -o $@ src/main.c $(PROTO) $(TABLE) $(WAL) $(LDFLAGS)

$(TEST_CLIENT): tests/test_client.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_client.c $(PROTO) $(LDFLAGS)

$(TEST_MULTI): tests/test_multi_client.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_multi_client.c $(PROTO) $(LDFLAGS)

# Talks to the store directly, with no network in the picture.
$(TEST_TABLE): tests/test_hashtable.c $(TABLE) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_hashtable.c $(TABLE) $(LDFLAGS)

$(TEST_STRESS): tests/test_thread_stress.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_thread_stress.c $(PROTO) $(LDFLAGS)

$(TEST_WAL): tests/test_wal.c $(WAL) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_wal.c $(WAL) $(LDFLAGS)

$(TEST_WAL_SERVER): tests/test_wal_server.c $(PROTO) $(WAL) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_wal_server.c $(PROTO) $(WAL) $(LDFLAGS)

$(TEST_RECOVERY): tests/test_recovery.c $(TABLE) $(WAL) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_recovery.c $(TABLE) $(WAL) $(LDFLAGS)

$(BENCHMARK): tests/benchmark.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/benchmark.c $(PROTO) $(LDFLAGS)

$(TSAN_SERVER): src/main.c $(PROTO) $(TABLE) $(WAL) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ src/main.c $(PROTO) $(TABLE) $(WAL) $(TSAN_LDFLAGS)

$(TSAN_TEST_MULTI): tests/test_multi_client.c $(PROTO) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ tests/test_multi_client.c $(PROTO) $(TSAN_LDFLAGS)

$(TSAN_TEST_TABLE): tests/test_hashtable.c $(TABLE) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ tests/test_hashtable.c $(TABLE) $(TSAN_LDFLAGS)

$(TSAN_TEST_STRESS): tests/test_thread_stress.c $(PROTO) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ tests/test_thread_stress.c $(PROTO) $(TSAN_LDFLAGS)

$(TSAN_TEST_WAL): tests/test_wal.c $(WAL) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ tests/test_wal.c $(WAL) $(TSAN_LDFLAGS)

$(TSAN_TEST_WAL_SERVER): tests/test_wal_server.c $(PROTO) $(WAL) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ tests/test_wal_server.c $(PROTO) $(WAL) $(TSAN_LDFLAGS)

$(TSAN_TEST_RECOVERY): tests/test_recovery.c $(TABLE) $(WAL) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ tests/test_recovery.c $(TABLE) $(WAL) $(TSAN_LDFLAGS)

$(TSAN_BENCHMARK): tests/benchmark.c $(PROTO) $(HDRS) | $(TSAN_BUILD)
	$(CC) $(TSAN_CFLAGS) -o $@ tests/benchmark.c $(PROTO) $(TSAN_LDFLAGS)

$(BUILD):
	mkdir -p $@

$(TSAN_BUILD):
	mkdir -p $@

clean:
	rm -rf $(BUILD)
