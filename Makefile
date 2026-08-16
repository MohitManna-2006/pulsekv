CC     ?= cc
CFLAGS := -Wall -Wextra -std=c11 -O2 -Iinclude -pthread

BUILD  := build

# Modules shared by the server and the tests that exercise them directly.
PROTO  := src/protocol.c
TABLE  := src/hashtable.c
HDRS   := include/protocol.h include/hashtable.h

SERVER      := $(BUILD)/pulsekv
SERVER_LT   := $(BUILD)/pulsekv_lt
TEST_CLIENT := $(BUILD)/test_client
TEST_MULTI  := $(BUILD)/test_multi_client
TEST_TABLE  := $(BUILD)/test_hashtable

.PHONY: all clean

all: $(SERVER) $(SERVER_LT) $(TEST_CLIENT) $(TEST_MULTI) $(TEST_TABLE)

$(SERVER): src/main.c $(PROTO) $(TABLE) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ src/main.c $(PROTO) $(TABLE)

# Level-triggered build of the same source. Step 2 is a progression from LT to
# ET, so keep both buildable and run the concurrency test against each.
$(SERVER_LT): src/main.c $(PROTO) $(TABLE) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -DPULSEKV_LEVEL_TRIGGERED -o $@ src/main.c $(PROTO) $(TABLE)

$(TEST_CLIENT): tests/test_client.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_client.c $(PROTO)

$(TEST_MULTI): tests/test_multi_client.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_multi_client.c $(PROTO)

# Talks to the store directly, with no network in the picture.
$(TEST_TABLE): tests/test_hashtable.c $(TABLE) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_hashtable.c $(TABLE)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)
