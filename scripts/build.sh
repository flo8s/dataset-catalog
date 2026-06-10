#!/usr/bin/env bash
set -euo pipefail
target="${1:-local}"
uv run fdl sync "$target"
