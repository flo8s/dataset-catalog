"""Fetch dbt artifacts + fdl.toml from S3 and build metadata JSON files.

For each datasource in datasources.yml:
  1. Download manifest.json, catalog.json from S3 ({ds}/dbt/)
  2. Download fdl.toml from S3 ({ds}/fdl.toml)
  3. Transform into metadata JSON (same schema as stg_* models expect)
  4. Write to .fdl/artifacts/{ds}.json
"""

from __future__ import annotations

import json
import os
import tomllib
from collections import defaultdict
from pathlib import Path

import yaml
from dbt.artifacts.resources.v1.model import Model
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.artifacts.schemas.manifest import WritableManifest

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATASOURCES_YML = PROJECT_DIR / "datasources.yml"
OUTPUT_DIR = PROJECT_DIR / ".fdl" / "artifacts"


def load_datasources() -> list[dict]:
    with open(DATASOURCES_YML) as f:
        return yaml.safe_load(f)["datasources"]


def create_s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["FDL_S3_ENDPOINT"],
        aws_access_key_id=os.environ["FDL_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["FDL_S3_SECRET_ACCESS_KEY"],
    )


def s3_get(client, bucket: str, key: str) -> bytes | None:
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except client.exceptions.NoSuchKey:
        print(f"    WARN: s3://{bucket}/{key} not found")
        return None


def parse_manifest(raw: str) -> WritableManifest:
    # Write to temp file because read_and_check_versions expects a path
    tmp = OUTPUT_DIR / "_tmp_manifest.json"
    tmp.write_text(raw)
    try:
        return WritableManifest.read_and_check_versions(str(tmp))
    finally:
        tmp.unlink(missing_ok=True)


def parse_catalog(raw: str) -> CatalogArtifact:
    tmp = OUTPUT_DIR / "_tmp_catalog.json"
    tmp.write_text(raw)
    try:
        return CatalogArtifact.read_and_check_versions(str(tmp))
    finally:
        tmp.unlink(missing_ok=True)


def resolve_column_type(
    col_name: str, col_info_data_type: str | None, catalog_columns: dict
) -> str:
    if col_name in catalog_columns:
        catalog_type = catalog_columns[col_name].type
        if catalog_type:
            return catalog_type
    return col_info_data_type or ""


def build_columns(node: Model, catalog_columns: dict) -> list[dict]:
    return [
        {
            "name": col_name,
            "title": col_info.meta.get("title", ""),
            "description": col_info.description,
            "data_type": resolve_column_type(
                col_name, col_info.data_type, catalog_columns
            ),
            "nullable": not any(
                c.type.value == "not_null" for c in col_info.constraints
            ),
        }
        for col_name, col_info in node.columns.items()
    ]


def build_model_info(
    node: Model, catalog_columns: dict, defaults: dict | None = None
) -> dict:
    meta = node.meta
    d = defaults or {}
    return {
        "name": node.name,
        "title": meta.get("title", ""),
        "description": node.description,
        "tags": meta.get("tags", []),
        "license": meta.get("license") or d.get("license", ""),
        "license_url": meta.get("license_url") or d.get("license_url", ""),
        "source_url": meta.get("source_url") or d.get("source_url", ""),
        "published": meta.get("published", False),
        "materialized": node.config.materialized,
        "columns": build_columns(node, catalog_columns),
        "sql": (node.compiled_code or "").strip() or None,
        "file_path": node.original_file_path or "",
    }


def extract_models(
    manifest: WritableManifest,
    catalog: CatalogArtifact | None,
    datasource: str,
    meta: dict | None = None,
) -> dict[str, list[dict]]:
    meta = meta or {}
    dataset_defaults = {
        k: meta[k] for k in ("license", "license_url", "source_url") if k in meta
    }
    schema_configs = meta.get("schemas", {})

    tables_by_schema: dict[str, list[dict]] = defaultdict(list)
    for node_id, node in manifest.nodes.items():
        if not isinstance(node, Model):
            continue
        if not node.fqn or node.fqn[0] != datasource:
            continue
        catalog_node = catalog.nodes.get(node_id) if catalog else None
        catalog_columns = catalog_node.columns if catalog_node else {}
        defaults = {**dataset_defaults, **{
            k: v
            for k, v in schema_configs.get(node.schema, {}).items()
            if k in ("license", "license_url", "source_url")
        }}
        tables_by_schema[node.schema].append(
            build_model_info(node, catalog_columns, defaults)
        )
    return dict(tables_by_schema)


def extract_lineage(manifest: WritableManifest, datasource: str) -> dict:
    prefix = f"model.{datasource}."
    parent_map_raw = manifest.parent_map or {}

    parent_map: dict[str, list[str]] = {}
    node_keys: set[str] = set()

    for full_key, parents in parent_map_raw.items():
        if not full_key.startswith(prefix):
            continue
        short_key = full_key[len(prefix) :]
        short_parents = [p[len(prefix) :] for p in parents if p.startswith(prefix)]
        parent_map[short_key] = short_parents
        node_keys.add(short_key)
        node_keys.update(short_parents)

    nodes: dict[str, dict] = {}
    for key in node_keys:
        full_key = prefix + key
        node = manifest.nodes.get(full_key)
        if node:
            nodes[key] = {
                "fqn": node.fqn,
                "resource_type": node.resource_type,
                "config": {"materialized": node.config.materialized},
                "meta": node.meta,
            }
        else:
            nodes[key] = {
                "fqn": [],
                "resource_type": "model",
                "config": {"materialized": "view"},
                "meta": {},
            }

    return {"parent_map": parent_map, "nodes": nodes}


def build_metadata(
    datasource: str,
    meta: dict,
    public_url: str,
    manifest: WritableManifest,
    catalog: CatalogArtifact | None,
    readme: str | None = None,
) -> dict:
    ducklake_url = f"{public_url}/{datasource}/ducklake.duckdb"
    tables_by_schema = extract_models(manifest, catalog, datasource, meta)
    lineage = extract_lineage(manifest, datasource)

    schemas = {}
    for name, schema_config in meta.get("schemas", {}).items():
        schemas[name] = {
            "title": schema_config.get("title", ""),
            "tables": tables_by_schema.get(name, []),
        }
    for name, tables in tables_by_schema.items():
        if name not in schemas:
            schemas[name] = {"title": "", "tables": tables}

    result = {
        "title": meta.get("title", ""),
        "description": meta.get("description", ""),
        "cover": meta.get("cover", ""),
        "tags": meta.get("tags", []),
        "ducklake_url": ducklake_url,
        "repository_url": meta.get("repository_url", ""),
        "schemas": schemas,
        "lineage": lineage,
    }
    if readme:
        result["readme"] = readme
    return result


def process_datasource(ds: dict, client, bucket: str) -> bool:
    name = ds["name"]
    print(f"  {name}:")

    manifest_raw = s3_get(client, bucket, f"{name}/dbt/manifest.json")
    if not manifest_raw:
        print(f"    SKIP: manifest.json not available")
        return False

    catalog_raw = s3_get(client, bucket, f"{name}/dbt/catalog.json")
    toml_raw = s3_get(client, bucket, f"{name}/fdl.toml")
    if not toml_raw:
        print(f"    SKIP: fdl.toml not available")
        return False

    config = tomllib.loads(toml_raw.decode())
    meta = config.get("meta", {})
    target = config.get("targets", {}).get("default", {})
    public_url = target.get("public_url", "https://data.queria.io")

    manifest = parse_manifest(manifest_raw.decode())
    catalog = parse_catalog(catalog_raw.decode()) if catalog_raw else None

    metadata = build_metadata(name, meta, public_url, manifest, catalog)

    output_path = OUTPUT_DIR / f"{name}.json"
    with open(output_path, "w") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    total_tables = sum(len(s["tables"]) for s in metadata["schemas"].values())
    print(f"    {total_tables} tables → {output_path.name}")
    return True


def process_datasource_local(ds: dict, base_dir: Path) -> bool:
    """Process a datasource from local filesystem instead of S3."""
    name = ds["name"]
    ds_dir = base_dir / name
    print(f"  {name} (local):")

    manifest_path = ds_dir / "dbt" / "manifest.json"
    if not manifest_path.exists():
        print(f"    SKIP: {manifest_path} not found")
        return False

    toml_path = ds_dir / "fdl.toml"
    if not toml_path.exists():
        print(f"    SKIP: {toml_path} not found")
        return False

    catalog_path = ds_dir / "dbt" / "catalog.json"

    config = tomllib.loads(toml_path.read_text())
    meta = config.get("meta", {})
    target = config.get("targets", {}).get("local", {})
    public_url = target.get("public_url", "http://localhost:4001")

    manifest = parse_manifest(manifest_path.read_text())
    catalog = (
        parse_catalog(catalog_path.read_text()) if catalog_path.exists() else None
    )

    metadata = build_metadata(name, meta, public_url, manifest, catalog)

    output_path = OUTPUT_DIR / f"{name}.json"
    with open(output_path, "w") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    total_tables = sum(len(s["tables"]) for s in metadata["schemas"].values())
    print(f"    {total_tables} tables → {output_path.name}")
    return True


def _detect_local_base_dir() -> Path | None:
    """Detect local mode from FDL_STORAGE environment variable.

    When run via `fdl run local`, FDL_STORAGE is set to a local path
    like ~/.local/share/fdl/catalog. The parent directory is the base
    where all datasets are stored.
    """
    storage = os.environ.get("FDL_STORAGE", "")
    if storage and not storage.startswith("s3://"):
        return Path(storage).parent
    return None


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    datasources = load_datasources()
    base_dir = _detect_local_base_dir()
    ok = 0

    if base_dir is not None:
        print(f"Local mode: reading from {base_dir}")
        for ds in datasources:
            if process_datasource_local(ds, base_dir):
                ok += 1
    else:
        client = create_s3_client()
        bucket = os.environ["FDL_S3_BUCKET"]
        for ds in datasources:
            if process_datasource(ds, client, bucket):
                ok += 1

    print(f"Built metadata for {ok}/{len(datasources)} datasources")


if __name__ == "__main__":
    main()
