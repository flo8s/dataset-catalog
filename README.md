## 概要

Queria で公開している全データセットのメタデータを統合したカタログデータセットです。
各データセットの dbt artifacts (manifest.json / catalog.json) と fdl.toml を直接読み込み、
テーブル定義・カラム定義・セマンティクス・リネージュ情報を一元管理しています。

## 主要テーブル

- mart_datasets: データセット一覧（タイトル、説明、タグ、DuckLake URL）
- mart_schemas: スキーマ一覧
- mart_tables: テーブル・ビュー定義（カラム情報、SQL、ライセンス等）
- mart_columns: カラム定義（名前、型、説明）
- mart_column_semantics: カラムのセマンティック役割（entity / dimension / measure）
- mart_entity_links: セマンティックエンティティのクロスデータセット結合関係
- mart_dependencies: データセット間の依存関係
- mart_search_entries: 全文検索用エントリ
- mart_lineage_nodes / mart_lineage_edges: データリネージュ情報

## パイプライン

1. `generate_sources.py` が `datasources.yml` を読み、以下を生成:
   - `.fdl/artifacts/{name}_meta.json` (fdl.toml → JSON 変換)
   - `models/main/raw/raw_{name}_*.sql` (マクロ呼び出しのみ)
   - `models/main/stg/stg_all_*.sql` (UNION ALL)
2. dbt が manifest.json / catalog.json を read_json で直接読み込み、stg/mart へ変換
3. `scripts/sync_catalog_d1.py` が mart テーブルを Cloudflare D1 に同期
