# megaton-app

GA4 / GSC / BigQuery のデータ取得・分析ツールキット。
Notebook・CLI・Streamlit UI の3つのインターフェースで使える。

## セットアップ

```bash
pip install -r requirements.txt

# credentials/ にサービスアカウント JSON を配置
# 複数ファイル可（property_id で自動ルーティング）
```

## 使い方

### Streamlit UI

```bash
streamlit run app/streamlit_app.py
# http://localhost:8501
```

`input/params.json` を監視し、変更を自動検出して UI に反映する。
AI エージェントが JSON を書き、人間が UI で確認・実行するワークフロー。

### CLI

```bash
# GA4 クエリ実行
python scripts/query.py ga4 --property 123456789 \
  --start today-30d --end today-1d \
  --dimensions date --metrics sessions

# バッチ実行（configs/ 内の JSON を順次処理）
python scripts/query.py --batch configs/
```

### Notebook

```python
from megaton_lib.megaton_client import get_ga4

mg = get_ga4("283927309")
mg.report.set.dates("2025-01-01", "2025-12-31")
mg.report.run(d=[...], m=[...])
df = mg.report.data
```

## 構成

```
megaton-app/
├── app/                # Streamlit UI
│   ├── streamlit_app.py
│   └── engine/         # GA4/GSC クエリ実行エンジン
├── megaton_lib/        # 共有ライブラリ（pip install -e で他リポジトリから利用）
├── scripts/            # CLI ツール
│   ├── query.py        # 統合クエリランナー
│   └── run_notebook.py # パラメータ付きノートブック実行
├── configs/            # クエリパラメータ JSON
├── credentials/        # サービスアカウント JSON（gitignore）
├── input/              # AI エージェント → UI パラメータ受け渡し
├── output/             # クエリ結果出力
├── tests/              # pytest テストスイート
└── docs/               # 詳細ドキュメント
```

## megaton_lib モジュール

他リポジトリ（megaton-notebooks 等）からも `pip install -e` で利用される共有パッケージ。

| モジュール | 役割 |
|---|---|
| `megaton_client.py` | GA4/GSC の初期化・クレデンシャル自動ルーティング |
| `credentials.py` | サービスアカウント JSON の自動検出（親ディレクトリ探索対応） |
| `date_template.py` | 相対日付式（`today-7d`, `prev-month-start` 等）の解決 |
| `articles.py` | GA4 記事メタデータの集約（言語別タイトル、カテゴリ等） |
| `periods.py` | 期間トークン（`2025`, `2025Q1`, `0`）のパース |
| `sheets.py` | Google Sheets テンプレート複製・書き込み |
| `params_validator.py` | JSON パラメータのスキーマバリデーション |
| `batch_runner.py` | 複数設定ファイルの順次実行 |
| `job_manager.py` | 非同期ジョブキュー管理 |
| `result_inspector.py` | クエリ結果の集計・パイプライン処理 |
| `analysis.py` | データ分析ユーティリティ |
| `notebook.py` | Notebook 初期化ヘルパー |

## テスト

```bash
pytest                          # 全テスト実行
pytest --cov=scripts.query      # カバレッジ付き（CI では 90% 以上必須）
```

## CI

- `tests.yml`: push/PR 時に pytest 実行（Python 3.12、カバレッジゲート 90%）

## 関連リポジトリ

| リポジトリ | 役割 |
|---|---|
| [megaton](https://github.com/mak00s/megaton) | GA4/GSC/Sheets API ラッパー（PyPI パッケージ） |
| [megaton-notebooks](https://github.com/mak00s/megaton-notebooks) | 定期レポート用ノートブック集 |
