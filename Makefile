CC     ?= cc
CFLAGS := -Wall -Wextra -std=c11 -O2 -Iinclude

BUILD  := build

PROTO  := src/protocol.c
HDRS   := include/protocol.h

SERVER      := $(BUILD)/pulsekv
TEST_CLIENT := $(BUILD)/test_client

.PHONY: all clean

all: $(SERVER) $(TEST_CLIENT)

$(SERVER): src/main.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ src/main.c $(PROTO)

$(TEST_CLIENT): tests/test_client.c $(PROTO) $(HDRS) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ tests/test_client.c $(PROTO)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)
