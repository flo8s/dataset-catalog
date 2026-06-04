#!/usr/bin/env bash
set -euo pipefail
target="${1:-local}"
uv run python main.py "$target"
