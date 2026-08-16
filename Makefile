CC     ?= cc
CFLAGS := -Wall -Wextra -std=c11 -O2 -Iinclude

BUILD  := build

PROTO  := src/protocol.c
HDRS   := include/protocol.h

SERVER      := $(BUILD)/pulsekv
SERVER_LT   := $(BUILD)/pulsekv_lt
TEST_CLIENT := $(BUILD)/test_client
TEST_MULTI  := $(BUILD)/test_multi_client

.PHONY: all clean

all: $(SERVER) $(SERVER_LT) $(TEST_CLIENT) $(TEST_MULTI)

$(SERVER): src/main.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ src/main.c $(PROTO)

# Level-triggered build of the same source. Step 2 is a progression from LT to
# ET, so keep both buildable and run the concurrency test against each.
$(SERVER_LT): src/main.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -DPULSEKV_LEVEL_TRIGGERED -o $@ src/main.c $(PROTO)

$(TEST_CLIENT): tests/test_client.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_client.c $(PROTO)

$(TEST_MULTI): tests/test_multi_client.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_multi_client.c $(PROTO)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)
