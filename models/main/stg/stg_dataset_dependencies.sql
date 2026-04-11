{{ config(materialized='view') }}

-- データセット間の依存関係（空テーブル）。
-- dbt の sources / refs は同一プロジェクト内に閉じており、cross-dataset ref
-- は存在しない（dbt mesh 非使用）。将来的に fdl.toml の [dependencies]
-- セクションから取得するか、DuckLake の ATTACH 宣言を解析するなどの実装が必要。
-- 下流（mart_dependencies, web の OG 画像生成）は 0 件で問題なく動作する。
SELECT
    ''::VARCHAR AS datasource,
    ''::VARCHAR AS alias,
    ''::VARCHAR AS ducklake_url
WHERE false
