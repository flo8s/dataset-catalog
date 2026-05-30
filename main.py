"""generate_sources + dbt build + snapshot pipeline.

Each dataset publishes its own meta.json and dbt artifacts to R2 via
dataset-shared/scripts/upload_artifacts.py. generate_sources.py fetches
those, materializes per-datasource raw models, then dbt builds the catalog
DuckLake in MotherDuck. Snapshot exports the catalog metadata back to R2
for queria-web to ATTACH.

Snapshot must run in the same Python process as dbt build — see
dataset-shared/README.md for the constraint detail.

Usage:
    python main.py [target]
    # target: "default" (default) or "local"
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

from dbt.cli.main import dbtRunner

SHARED_SCRIPTS = Path(__file__).resolve().parent / "shared" / "scripts"
_spec = importlib.util.spec_from_file_location(
    "snapshot_to_r2", SHARED_SCRIPTS / "snapshot-to-r2.py"
)
assert _spec and _spec.loader
snapshot_to_r2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(snapshot_to_r2)


def main() -> None:
    target = os.environ.get("DBT_TARGET", sys.argv[1] if len(sys.argv) > 1 else "default")

    # 1. Auto-generate raw model SQL + fetch each dataset's meta.json from R2
    from generate_sources import main as gen
    gen()

    # 2. dbt build (writes to MotherDuck catalog DuckLake)
    dbt = dbtRunner()
    for cmd in (["deps"], ["run", "--target", target]):
        result = dbt.invoke(cmd)
        if not result.success:
            raise SystemExit(f"dbt {' '.join(cmd)} failed")

    # 3. snapshot MotherDuck catalog to R2 (same process — required by MD constraint)
    snapshot_to_r2.run(target)


if __name__ == "__main__":
    main()
