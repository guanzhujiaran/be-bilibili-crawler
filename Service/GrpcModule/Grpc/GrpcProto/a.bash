#!/bin/bash
set -euo pipefail

TARGET_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# GrpcProto -> Grpc -> GrpcModule -> Service -> be-bilibili-crawler（项目根，含 pyproject.toml 与 .venv）
PROJECT_DIR="$(cd "$TARGET_DIR/../../../.." && pwd)"

echo "Deleting existing .py files in $TARGET_DIR ..."
find "$TARGET_DIR" -name "*.py" -type f -delete

echo "Compiling .proto files..."
find "$TARGET_DIR" -name "*.proto" -type f | while read -r proto_file; do
    uv run --directory "$PROJECT_DIR" python -m grpc_tools.protoc \
    --proto_path="$TARGET_DIR" \
    --python_out="$TARGET_DIR" \
    --grpc_python_out="$TARGET_DIR" \
    "$proto_file"
done

echo "Done."
