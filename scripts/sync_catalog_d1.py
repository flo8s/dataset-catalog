"""Sync catalog data from DuckLake (R2) to Cloudflare D1.

DuckDB で R2 上の catalog DuckLake に接続し、mart テーブルから
D1 用の SQL を生成して Cloudflare Python SDK で実行する。

Usage:
    uv run --with cloudflare python scripts/sync_catalog_d1.py
    uv run --with cloudflare python scripts/sync_catalog_d1.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

R2_PUBLIC_URL = "https://data.queria.io"
CATALOG_DUCKLAKE_URL = f"{R2_PUBLIC_URL}/catalog/ducklake.duckdb"
CATALOG_ALIAS = "catalog"

D1_TABLES = [
    "catalog_datasets",
    "catalog_schemas",
    "catalog_nodes",
    "catalog_columns",
    "catalog_column_semantics",
]


# --- SQL helpers ---


def esc_sql(s: str) -> str:
    return s.replace("'", "''")


def sql_val(v: object) -> str:
    if v is None:
        return "NULL"
    return f"'{esc_sql(str(v))}'"


def json_val(v: object) -> str:
    if v is None:
        return "NULL"
    return f"'{esc_sql(json.dumps(v, ensure_ascii=False))}'"


def bool_val(v: object) -> str:
    return "1" if v is True else "0"


def build_insert(table: str, row: dict[str, str]) -> str:
    cols = ", ".join(row.keys())
    vals = ", ".join(row.values())
    return f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({vals});"


# --- SQL generation ---


def generate_catalog_sql() -> str:
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute("SET http_retries = 10")
    conn.execute("SET http_retry_wait_ms = 1000")
    conn.execute("SET http_retry_backoff = 2.0")
    conn.execute(
        f"ATTACH 'ducklake:{CATALOG_DUCKLAKE_URL}' AS {CATALOG_ALIAS} (READ_ONLY)"
    )

    statements: list[str] = [f"DELETE FROM {t};" for t in D1_TABLES]

    # --- Datasets ---
    datasets = conn.execute(
        f"""
        SELECT datasource, title, description, cover, ducklake_url, repository_url,
               schedule, tags_json, dbt_version, dbt_generated_at
        FROM {CATALOG_ALIAS}.main.mart_datasets
        ORDER BY datasource
        """
    ).fetchall()
    ds_columns = [
        "datasource", "title", "description", "cover", "ducklake_url",
        "repository_url", "schedule", "tags_json", "dbt_version", "dbt_generated_at",
    ]

    for row in datasets:
        r = dict(zip(ds_columns, row))
        tags = json.loads(str(r["tags_json"])) if r["tags_json"] else None
        statements.append(
            build_insert(
                "catalog_datasets",
                {
                    "datasource": sql_val(r["datasource"]),
                    "title": sql_val(r["title"]),
                    "description": sql_val(r["description"]),
                    "cover": sql_val(r["cover"]),
                    "ducklake_url": sql_val(r["ducklake_url"]),
                    "repository_url": sql_val(r["repository_url"]),
                    "schedule": sql_val(r["schedule"]),
                    "tags": json_val(tags),
                    "dbt_version": sql_val(r["dbt_version"]),
                    "dbt_generated_at": sql_val(r["dbt_generated_at"]),
                },
            )
        )

    # --- Schemas ---
    schemas = conn.execute(
        f"""
        SELECT datasource, schema_name, title
        FROM {CATALOG_ALIAS}.main.mart_schemas
        ORDER BY datasource, schema_name
        """
    ).fetchall()
    for row in schemas:
        datasource, schema_name, title = row
        schema_id = f"{datasource}/{schema_name}"
        statements.append(
            build_insert(
                "catalog_schemas",
                {
                    "id": sql_val(schema_id),
                    "datasource": sql_val(datasource),
                    "schema_name": sql_val(schema_name),
                    "title": sql_val(title or ""),
                },
            )
        )

    # --- Nodes (全 resource_type だが web 向けには model のみに絞る) ---
    nodes = conn.execute(
        f"""
        SELECT datasource, unique_id, resource_type, name, schema_name, database,
               description, materialized, title, license, license_url, source_url,
               is_published, tags_json, compiled_code, file_path, node_index
        FROM {CATALOG_ALIAS}.main.mart_nodes
        WHERE resource_type = 'model'
        ORDER BY datasource, node_index
        """
    ).fetchall()
    node_columns = [
        "datasource", "unique_id", "resource_type", "name", "schema_name", "database",
        "description", "materialized", "title", "license", "license_url", "source_url",
        "is_published", "tags_json", "compiled_code", "file_path", "node_index",
    ]
    for row in nodes:
        r = dict(zip(node_columns, row))
        tags = json.loads(str(r["tags_json"])) if r["tags_json"] else None
        statements.append(
            build_insert(
                "catalog_nodes",
                {
                    "unique_id": sql_val(r["unique_id"]),
                    "datasource": sql_val(r["datasource"]),
                    "resource_type": sql_val(r["resource_type"]),
                    "name": sql_val(r["name"]),
                    "schema_name": sql_val(r["schema_name"]),
                    "database": sql_val(r["database"]),
                    "description": sql_val(r["description"] or ""),
                    "materialized": sql_val(r["materialized"]),
                    "title": sql_val(r["title"] or ""),
                    "license": sql_val(r["license"]),
                    "license_url": sql_val(r["license_url"]),
                    "source_url": sql_val(r["source_url"]),
                    "is_published": bool_val(r["is_published"]),
                    "tags": json_val(tags),
                    "compiled_code": sql_val(r["compiled_code"]),
                    "file_path": sql_val(r["file_path"]),
                    "node_index": str(r["node_index"] or 0),
                },
            )
        )

    # --- Columns ---
    columns = conn.execute(
        f"""
        SELECT c.datasource, c.unique_id, c.table_name, c.column_name,
               c.title, c.description, c.data_type, c.column_index
        FROM {CATALOG_ALIAS}.main.mart_columns c
        JOIN {CATALOG_ALIAS}.main.mart_nodes n
          ON c.unique_id = n.unique_id AND c.datasource = n.datasource
        WHERE n.resource_type = 'model'
        ORDER BY c.datasource, c.column_index
        """
    ).fetchall()
    col_columns = [
        "datasource", "unique_id", "table_name", "column_name",
        "title", "description", "data_type", "column_index",
    ]
    for row in columns:
        r = dict(zip(col_columns, row))
        col_id = f"{r['unique_id']}/{r['column_name']}"
        statements.append(
            build_insert(
                "catalog_columns",
                {
                    "id": sql_val(col_id),
                    "datasource": sql_val(r["datasource"]),
                    "unique_id": sql_val(r["unique_id"]),
                    "table_name": sql_val(r["table_name"]),
                    "column_name": sql_val(r["column_name"]),
                    "title": sql_val(r["title"]),
                    "description": sql_val(r["description"] or ""),
                    "data_type": sql_val(r["data_type"]),
                    "column_index": str(r["column_index"] or 0),
                },
            )
        )

    # --- Column Semantics ---
    try:
        semantics = conn.execute(
            f"""
            SELECT cs.datasource, cs.unique_id, cs.column_name,
                   cs.semantic_role, cs.semantic_name, cs.semantic_type, cs.agg
            FROM {CATALOG_ALIAS}.main.mart_column_semantics cs
            ORDER BY cs.datasource, cs.unique_id, cs.column_name
            """
        ).fetchall()
    except duckdb.CatalogException as e:
        print(f"WARNING: skipping mart_column_semantics: {e}")
        semantics = []

    sem_columns = [
        "datasource", "unique_id", "column_name",
        "semantic_role", "semantic_name", "semantic_type", "agg",
    ]
    for row in semantics:
        r = dict(zip(sem_columns, row))
        sem_id = f"{r['unique_id']}/{r['column_name'] or '_'}/{r['semantic_role']}/{r['semantic_name']}"
        statements.append(
            build_insert(
                "catalog_column_semantics",
                {
                    "id": sql_val(sem_id),
                    "datasource": sql_val(r["datasource"]),
                    "unique_id": sql_val(r["unique_id"]),
                    "column_name": sql_val(r["column_name"]),
                    "semantic_role": sql_val(r["semantic_role"]),
                    "semantic_name": sql_val(r["semantic_name"]),
                    "semantic_type": sql_val(r["semantic_type"]),
                    "agg": sql_val(r["agg"]),
                },
            )
        )

    conn.close()

    return "\n".join(["-- Catalog data", *statements])


# --- D1 execution ---


def execute_d1(sql: str) -> None:
    from cloudflare import Cloudflare

    client = Cloudflare(api_token=os.environ["CF_API_TOKEN"])
    account_id = os.environ["CF_ACCOUNT_ID"]
    database_id = os.environ["CF_D1_DATABASE_ID"]

    schema_path = Path(__file__).parent / "d1_schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")
    schema_stmts = [
        s.strip()
        for s in schema_sql.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]
    bootstrap_sql = "; ".join(schema_stmts) + ";"
    print(f"  applying schema ({len(schema_stmts)} statements)...")
    client.d1.database.query(
        database_id=database_id,
        account_id=account_id,
        sql=bootstrap_sql,
    )

    stmts = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    chunk_size = 200

    for i in range(0, len(stmts), chunk_size):
        chunk = stmts[i : i + chunk_size]
        batch_sql = "; ".join(chunk) + ";"
        client.d1.database.query(
            database_id=database_id,
            account_id=account_id,
            sql=batch_sql,
        )
        print(f"  executed {len(chunk)} statements ({i + 1}-{i + len(chunk)})")


# --- CLI ---


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync catalog to Cloudflare D1")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate SQL and print to stdout without executing",
    )
    args = parser.parse_args()

    print("Generating catalog SQL...")
    sql = generate_catalog_sql()

    if args.dry_run:
        print(sql)
        return

    print("Executing SQL on D1...")
    execute_d1(sql)
    print("Done.")


if __name__ == "__main__":
    main()
