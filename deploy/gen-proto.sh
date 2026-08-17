#!/usr/bin/env bash
#
# Regenerate the v2 protobuf/gRPC stubs.
#
# Run this inside the v2 dev image so the pinned plugin versions are what
# actually produced the checked-in files:
#
#   docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev deploy/gen-proto.sh
#
#   deploy/gen-proto.sh          Go + Python (the checked-in outputs)
#   deploy/gen-proto.sh --all    ... plus a throwaway C++ generation check
#
# C++ is not generated into the tree here: node/grpc_shim/CMakeLists.txt does
# that at build time, because .pb.cc is ABI-coupled to the libprotobuf it links
# against. --all runs protoc's C++ backend into a temp directory purely to
# prove all three .proto files generate cleanly for C++ too, which is one of
# Phase 0's exit criteria.
#
# See proto/README.md for the checked-in-vs-generated policy.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

CHECK_CPP=0
for arg in "$@"; do
    case "$arg" in
        --all) CHECK_CPP=1 ;;
        -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) pk_die "unknown argument: $arg" ;;
    esac
done

cd "$PULSEKV_REPO_ROOT"

PROTO_DIR="proto"
PROTO_FILES=("$PROTO_DIR/node.proto" "$PROTO_DIR/metadata.proto" "$PROTO_DIR/adapter.proto")
PY_OUT="adapters/pulsekv_adapters/gen"

for f in "${PROTO_FILES[@]}"; do
    [ -f "$f" ] || pk_die "missing $f"
done

# ---------------------------------------------------------------------------
# Go
# ---------------------------------------------------------------------------
pk_step "Generating Go stubs -> control/gen/"

pk_require protoc "Install the v2 dev image (deploy/Dockerfile)."
pk_require protoc-gen-go "Install the v2 dev image (deploy/Dockerfile)."
pk_require protoc-gen-go-grpc "Install the v2 dev image (deploy/Dockerfile)."

# --go_opt=module=pulsekv strips the `pulsekv/` prefix from each file's
# go_package, so `pulsekv/control/gen/node/v1` lands at control/gen/node/v1
# relative to the repo root. That path is also the import path, because
# control/go.mod declares `module pulsekv/control`.
rm -rf control/gen
protoc -I "$PROTO_DIR" \
    --go_out=. --go_opt=module=pulsekv \
    --go-grpc_out=. --go-grpc_opt=module=pulsekv \
    "${PROTO_FILES[@]}"

pk_dim "protoc          $(protoc --version | awk '{print $2}')"
pk_dim "protoc-gen-go   $(protoc-gen-go --version 2>&1 | awk '{print $2}')"
pk_dim "protoc-gen-go-grpc $(protoc-gen-go-grpc --version 2>&1 | awk '{print $2}')"
find control/gen -name '*.go' | sort | while read -r f; do pk_ok "$f"; done

# ---------------------------------------------------------------------------
# Python
# ---------------------------------------------------------------------------
pk_step "Generating Python stubs -> $PY_OUT/"

python3 -c 'import grpc_tools.protoc' 2>/dev/null || \
    pk_die "grpcio-tools not installed. Use the v2 dev image, or: pip install 'grpcio-tools==1.83.0'"

rm -rf "$PY_OUT"
mkdir -p "$PY_OUT"
python3 -m grpc_tools.protoc -I "$PROTO_DIR" \
    --python_out="$PY_OUT" \
    --pyi_out="$PY_OUT" \
    --grpc_python_out="$PY_OUT" \
    "${PROTO_FILES[@]}"

cat > "$PY_OUT/__init__.py" <<'EOF'
"""Generated protobuf/gRPC stubs for the PulseKV v2 contract.

DO NOT EDIT. Regenerate with deploy/gen-proto.sh.

Checked in so `pip install ./adapters` works without protoc on the machine.
See proto/README.md.
"""
EOF

# grpc_tools emits `import node_pb2 as node__pb2` -- a top-level absolute
# import that breaks the moment the generated modules live inside a package.
# Rewriting it to a relative import is the standard workaround for
# protocolbuffers/protobuf#1491. Done here, mechanically, never by hand.
rewritten=0
for f in "$PY_OUT"/*_pb2_grpc.py; do
    [ -e "$f" ] || continue
    tmp="$f.tmp"
    sed -E 's/^import ([A-Za-z_][A-Za-z0-9_]*_pb2) as /from . import \1 as /' "$f" > "$tmp"
    if ! cmp -s "$f" "$tmp"; then rewritten=$((rewritten + 1)); fi
    mv "$tmp" "$f"
done

pk_dim "grpcio-tools    $(python3 -c 'import grpc_tools; print(grpc_tools.__version__)' 2>/dev/null || pip show grpcio-tools | awk '/^Version:/{print $2}')"
pk_dim "rewrote package-relative imports in $rewritten file(s)"
find "$PY_OUT" -name '*.py' -o -name '*.pyi' | sort | while read -r f; do pk_ok "$f"; done

# Import the generated package to prove it actually loads -- a stub that
# generates but does not import is not generated.
PYTHONPATH="adapters${PYTHONPATH:+:$PYTHONPATH}" python3 - <<'EOF'
from pulsekv_adapters.gen import adapter_pb2_grpc, metadata_pb2_grpc, node_pb2_grpc  # noqa: F401
print("    import check: pulsekv_adapters.gen loads cleanly")
EOF

# ---------------------------------------------------------------------------
# C++ (verification only)
# ---------------------------------------------------------------------------
if [ "$CHECK_CPP" -eq 1 ]; then
    pk_step "Verifying C++ generation (throwaway output)"
    pk_require grpc_cpp_plugin "Install the v2 dev image (deploy/Dockerfile)."

    tmp_cpp="$(mktemp -d)"
    trap 'rm -rf "$tmp_cpp"' EXIT

    protoc -I "$PROTO_DIR" \
        --cpp_out="$tmp_cpp" \
        --grpc_out="$tmp_cpp" \
        --plugin=protoc-gen-grpc="$(command -v grpc_cpp_plugin)" \
        "${PROTO_FILES[@]}"

    pk_dim "libprotobuf     $(pkg-config --modversion protobuf 2>/dev/null || echo '?')"
    pk_dim "grpc++          $(pkg-config --modversion grpc++ 2>/dev/null || echo '?')"
    find "$tmp_cpp" -type f | sort | while read -r f; do pk_ok "$(basename "$f")"; done
    pk_dim "discarded; node/grpc_shim/CMakeLists.txt regenerates these at build time"
fi

pk_step "Done"
pk_info "Go stubs and Python stubs are checked in -- review and commit the diff."
