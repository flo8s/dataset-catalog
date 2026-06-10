"""generate_sources + dbt ビルドパイプライン。

manifest.json / catalog.json / semantic_manifest.json は各データセットの
upload_artifacts.py により storage base ( FDL_DATA_URL から導出 ) に配置済み。
fdl.toml は fdl push により配置済み。
generate_sources.py が raw モデルと meta JSON を生成し、
dbt が read_json で直接読み込む。
"""

from dbt.cli.main import dbtRunner


def main():
    # ソース定義と meta JSON を自動生成
    # 副作用: FDL_STORAGE_BASE 環境変数をセット（dbt マクロが使う）
    from generate_sources import main as gen

    gen()

    result = dbtRunner().invoke(["run"])
    if not result.success:
        raise SystemExit("dbt run failed")


if __name__ == "__main__":
    main()
