# megaton-app

GA4 / Search Console / BigQuery のデータ取得・分析ツールキット。
Notebook・CLI・Streamlit UI の3つのインターフェースで使える。

## セットアップ

```bash
pip install -r requirements.txt
# credentials/ にサービスアカウント JSON を配置（複数可、property_id で自動ルーティング）
```

## クイックスタート

```bash
# Streamlit UI
streamlit run app/streamlit_app.py

# CLI（params.json の source で GA4/GSC/BigQuery を自動判定）
python scripts/query.py --params input/params.json

# テスト
pytest
```

詳しい使い方は [docs/USAGE.md](docs/USAGE.md) を参照。

## 構成

```
megaton-app/
├── megaton_lib/        # 共有ライブラリ（pip install -e で他リポジトリから利用）
├── scripts/            # CLI（query.py, run_notebook.py）
├── app/                # Streamlit UI
├── notebooks/          # Jupyter Notebook（Jupytext .py ↔ .ipynb）
├── credentials/        # サービスアカウント JSON（Git管理外）
├── configs/            # バッチ実行用 JSON
├── input/              # AI Agent → UI パラメータ受け渡し
├── output/             # クエリ結果・ジョブ管理
├── tests/              # pytest（CI で 90% カバレッジ必須）
└── docs/               # 詳細ドキュメント
```

## ドキュメント

| ドキュメント | 内容 |
|-------------|------|
| [docs/USAGE.md](docs/USAGE.md) | Notebook・CLI・Streamlit UI の使い方ガイド |
| [docs/REFERENCE.md](docs/REFERENCE.md) | JSONスキーマ、megaton API、認証、パイプライン |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | 変更履歴 |

## 関連リポジトリ

| リポジトリ | 役割 |
|---|---|
| [megaton](https://github.com/mak00s/megaton) | GA4/GSC/Sheets API ラッパー（PyPI パッケージ） |
| [megaton-notebooks](https://github.com/mak00s/megaton-notebooks) | 定期レポート用ノートブック集 |
