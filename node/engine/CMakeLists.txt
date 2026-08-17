# PulseKV v2 -- storage engine (pure C).
#
# Builds as a standalone project (cmake -S node/engine -B <dir>) so the engine
# and its tests can be worked on with no gRPC toolchain present at all, and as
# an add_subdirectory() of node/grpc_shim, which is how the node binary gets it.
#
# Nothing here links, includes, or knows about gRPC or protobuf. That is the
# whole point of the boundary -- see node/README.md.

cmake_minimum_required(VERSION 3.16)
project(pulsekv_engine C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(PULSEKV_BUILD_TESTS "Build the engine test binaries" ON)

# none | thread | address
#
# v1 kept a parallel ThreadSanitizer build of its whole suite because "the tests
# pass" is not evidence for a concurrent data structure. This engine has more
# shared mutable state than v1's table did -- an LRU list and four counters per
# shard on top of the chains -- so the same discipline applies.
set(PULSEKV_SANITIZE "none" CACHE STRING "Sanitizer for the engine build: none, thread, or address")
set_property(CACHE PULSEKV_SANITIZE PROPERTY STRINGS none thread address)

find_package(Threads REQUIRED)

add_library(pulsekv_engine STATIC
  src/engine.c
  src/hashtable.c
  src/tiering.c
)

# include/ is PUBLIC: pulsekv_engine.h is the contract.
# src/    is PRIVATE: hashtable.h and tiering.h are not for consumers. This is
#         what mechanically enforces "the shim includes exactly one header".
target_include_directories(pulsekv_engine
  PUBLIC  "${CMAKE_CURRENT_SOURCE_DIR}/include"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
)

target_compile_options(pulsekv_engine PRIVATE
  -Wall -Wextra -Wshadow -Wvla -Wpointer-arith -Wwrite-strings
)
target_link_libraries(pulsekv_engine PUBLIC Threads::Threads)

if(NOT PULSEKV_SANITIZE STREQUAL "none")
  message(STATUS "pulsekv: engine built with -fsanitize=${PULSEKV_SANITIZE}")
  target_compile_options(pulsekv_engine PUBLIC -fsanitize=${PULSEKV_SANITIZE} -g -fno-omit-frame-pointer)
  target_link_options(pulsekv_engine PUBLIC -fsanitize=${PULSEKV_SANITIZE})
endif()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(PULSEKV_BUILD_TESTS)
  enable_testing()

  # The tests reach into src/ deliberately: they exercise the table and the tier
  # directly as well as through the public API, the same way v1's
  # test_hashtable.c talked to the store with no network in the picture.
  set(PULSEKV_ENGINE_TESTS
    test_engine_basic
    test_engine_chunked
    test_engine_tiering
    test_engine_stress
  )

  foreach(t IN LISTS PULSEKV_ENGINE_TESTS)
    add_executable(${t} "tests/${t}.c")
    target_link_libraries(${t} PRIVATE pulsekv_engine)
    target_include_directories(${t} PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/src"
      "${CMAKE_CURRENT_SOURCE_DIR}/tests"
    )
    target_compile_options(${t} PRIVATE -Wall -Wextra)
    # mkdtemp/rmdir/unlink under -std=c11 with extensions off.
    target_compile_definitions(${t} PRIVATE _POSIX_C_SOURCE=200809L)
    add_test(NAME ${t} COMMAND ${t})
    # The stress and tiering tests do real disk I/O under sanitizers; the
    # default 1500s ctest timeout is generous but make the intent explicit.
    set_tests_properties(${t} PROPERTIES TIMEOUT 600)
  endforeach()
endif()
