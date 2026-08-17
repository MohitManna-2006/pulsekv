# PulseKV v2 -- data-plane node gRPC shim.
#
# CMake rather than a hand-written Makefile (which is what v1 uses for its pure
# C build) because gRPC C++'s official build guidance assumes CMake, and the
# link line for grpc++ is 100+ abseil libraries deep. Reproducing that by hand
# buys nothing.
#
# Dependencies are found via pkg-config rather than gRPC's own CMake config
# package: Debian ships grpc++.pc and protobuf.pc reliably, while the CMake
# config files vary between the distro packages and a from-source gRPC install.
# pkg-config works with both.
#
# Build:
#   cmake -S node/grpc_shim -B <builddir> -DCMAKE_BUILD_TYPE=Release
#   cmake --build <builddir> -j
#
# deploy/run-local-cluster.sh does exactly that; nothing here is meant to be
# run by hand.

cmake_minimum_required(VERSION 3.16)
project(pulsekv_node_shim CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
find_package(Threads REQUIRED)

find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

message(STATUS "pulsekv: protobuf ${PROTOBUF_VERSION}, grpc++ ${GRPCPP_VERSION}")
message(STATUS "pulsekv: protoc ${PROTOC_EXECUTABLE}, plugin ${GRPC_CPP_PLUGIN}")

# ---------------------------------------------------------------------------
# Generated protobuf/gRPC C++ sources.
#
# These are NOT checked in: .pb.cc is tied to the exact libprotobuf ABI it
# links against, so it is regenerated against whatever the local toolchain is.
# See proto/README.md.
# ---------------------------------------------------------------------------

set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../proto")
set(GEN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/gen")

# All three are generated, not just node.proto, and two of them are load-bearing
# now rather than only proving the contract compiles:
#
#   node      -- the service this process implements, and (since Phase 4) also
#                the client stub it uses to forward writes to its replica peers.
#   metadata  -- Phase 4's topology poller is a real ClusterMetadataService
#                CLIENT. Phase 0 generated this purely to prove the whole
#                contract compiles under protoc's C++ backend; the shim now
#                links and calls it.
#   adapter   -- still generation-only. Nothing here implements or calls it, but
#                a compile error in adapter.proto is far cheaper to find now than
#                in Phase 7.
#
# All of them land in the pulsekv_proto static library below, which pulsekv-node
# links, so no per-proto wiring is needed when one of them starts being used.
set(PROTO_NAMES node metadata adapter)

# Composed as one variable rather than written inline as
# `--plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"`: that form starts unquoted,
# so CMake keeps the inner quote characters literally and protoc ends up trying
# to exec a path that has `"` in it.
set(GRPC_PLUGIN_ARG "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}")

set(PROTO_SRCS "")
foreach(name IN LISTS PROTO_NAMES)
  set(pb_cc "${GEN_DIR}/${name}.pb.cc")
  set(pb_h "${GEN_DIR}/${name}.pb.h")
  set(grpc_cc "${GEN_DIR}/${name}.grpc.pb.cc")
  set(grpc_h "${GEN_DIR}/${name}.grpc.pb.h")

  add_custom_command(
    OUTPUT "${pb_cc}" "${pb_h}" "${grpc_cc}" "${grpc_h}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${GEN_DIR}"
    COMMAND "${PROTOC_EXECUTABLE}"
            -I "${PROTO_DIR}"
            --cpp_out "${GEN_DIR}"
            --grpc_out "${GEN_DIR}"
            "${GRPC_PLUGIN_ARG}"
            "${PROTO_DIR}/${name}.proto"
    DEPENDS "${PROTO_DIR}/${name}.proto"
    COMMENT "protoc (C++) ${name}.proto"
    VERBATIM)

  list(APPEND PROTO_SRCS "${pb_cc}" "${grpc_cc}")
endforeach()

add_library(pulsekv_proto STATIC ${PROTO_SRCS})
target_include_directories(pulsekv_proto PUBLIC "${GEN_DIR}")
target_link_libraries(pulsekv_proto PUBLIC PkgConfig::GRPCPP PkgConfig::PROTOBUF)
# Generated code is not ours to keep warning-clean.
target_compile_options(pulsekv_proto PRIVATE -w)

# ---------------------------------------------------------------------------
# The shim binary.
# ---------------------------------------------------------------------------

# The Phase 1 seam, now closed. node/engine/ is a self-contained pure-C CMake
# project; it knows nothing about gRPC or protobuf and is buildable and
# testable on its own (cmake -S node/engine).
#
# The include scoping is what mechanically enforces the boundary rather than
# leaving it to discipline: pulsekv_engine exports include/ as PUBLIC and src/
# as PRIVATE, so pulsekv_engine.h is the only engine header main.cpp can reach.
# hashtable.h and tiering.h are not on its include path at all, and a stray
# #include of one fails to compile.
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../engine" engine)

add_executable(pulsekv-node main.cpp)
target_compile_options(pulsekv-node PRIVATE -Wall -Wextra)
target_link_libraries(pulsekv-node PRIVATE pulsekv_proto pulsekv_engine Threads::Threads)

# Server reflection lets grpcurl introspect a running node without the .proto
# files. Optional: the shim compiles and serves identically without it, so a
# toolchain that lacks the library is not a build failure.
#
# --no-as-needed is required because nothing in our object files references
# the reflection library directly -- it registers itself through a static
# initialiser, which the linker would otherwise drop.
find_library(GRPCPP_REFLECTION_LIB NAMES grpc++_reflection)
if(GRPCPP_REFLECTION_LIB)
  message(STATUS "pulsekv: server reflection enabled (${GRPCPP_REFLECTION_LIB})")
  target_compile_definitions(pulsekv-node PRIVATE PULSEKV_HAVE_GRPC_REFLECTION=1)
  target_link_libraries(pulsekv-node PRIVATE
    "-Wl,--no-as-needed" "${GRPCPP_REFLECTION_LIB}" "-Wl,--as-needed")
else()
  message(STATUS "pulsekv: grpc++_reflection not found, building without reflection")
  target_compile_definitions(pulsekv-node PRIVATE PULSEKV_HAVE_GRPC_REFLECTION=0)
endif()
