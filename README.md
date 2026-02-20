# megaton-app

GA4 / GSC / BigQuery のデータ取得・分析ツールキット。
Notebook・CLI・Streamlit UI の3つのインターフェースで使える。

## セットアップ

```bash
pip install -r requirements.txt

# credentials/ にサービスアカウント JSON を配置
# 複数ファイル可（property_id で自動ルーティング）
```

### トラブルシューティング

セットアップ時に問題が発生した場合：

- **認証エラーが発生する場合**: サービスアカウントJSONファイルが正しく配置され、GA4/GSC/BigQueryの権限が付与されているか確認してください
- **モジュールが見つからない場合**: `pip install -r requirements.txt` を再実行してください
- **その他の問題**: [Issues](https://github.com/mak00s/megaton-app/issues) でサポートを受けられます
- **詳細なガイド**: [docs/USAGE.md](docs/USAGE.md) に詳しい使い方があります

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
# params.json から同期実行（source=ga4/gsc/bigquery を自動判定）
python scripts/query.py --params input/params.json

# 非同期ジョブ投入
python scripts/query.py --submit --params input/params.json

# バッチ実行（configs/ 内の JSON を順次処理）
python scripts/query.py --batch configs/
```

### Notebook

```python
from megaton_lib.megaton_client import get_ga4

mg = get_ga4("283927309")
mg.report.set.dates("2025-01-01", "2025-12-31")
result = mg.report.run(d=[...], m=[...], show=False)
df = result.df
```

## 構成

```
megaton-app/
├── app/                # Streamlit UI
│   ├── streamlit_app.py          # メインUIアプリケーション
│   └── engine/                   # GA4/GSC クエリ実行エンジン
├── megaton_lib/        # 共有ライブラリ（pip install -e で他リポジトリから利用）
│   ├── megaton_client.py         # GA4/GSC初期化
│   ├── credentials.py            # 認証情報管理
│   ├── slqm_analysis.py          # SLQM分析ヘルパー
│   └── ...                       # その他モジュール（下記参照）
├── scripts/            # CLI ツール
│   ├── query.py                  # 統合クエリランナー（GA4/GSC/BigQuery）
│   └── run_notebook.py           # パラメータ付きノートブック実行
├── configs/            # クエリパラメータ JSON
│   ├── weekly/                   # 週次バッチ設定
│   └── monthly/                  # 月次バッチ設定
├── credentials/        # サービスアカウント JSON（gitignore）
│   └── *.json                    # sa-xxx-yyy.json 形式
├── input/              # AI エージェント → UI パラメータ受け渡し
│   └── params.json               # Streamlit UI が監視
├── output/             # クエリ結果出力
│   ├── jobs/                     # ジョブ管理（records/logs/artifacts）
│   └── *.csv                     # クエリ結果CSV
├── tests/              # pytest テストスイート
│   ├── test_query.py             # CLIテスト
│   └── ...
└── docs/               # 詳細ドキュメント
    ├── USAGE.md                  # 使い方ガイド
    ├── PROGRESS.md               # 進捗履歴
    └── REFERENCE.md              # 技術リファレンス
```

## megaton_lib モジュール

他リポジトリ（megaton-notebooks 等）からも `pip install -e` で利用される共有パッケージ。

### コアモジュール（汎用）

| モジュール | 役割 | 主な関数/クラス |
|---|---|---|
| `megaton_client.py` | GA4/GSC の初期化・クレデンシャル自動ルーティング | `get_ga4()`, `get_gsc()`, `query_ga4()`, `query_gsc()`, `query_bq()` |
| `credentials.py` | サービスアカウント JSON の自動検出（親ディレクトリ探索対応） | `build_registry()`, `find_creds()` |
| `date_template.py` | 相対日付式（`today-7d`, `prev-month-start` 等）の解決 | `resolve_date()` |
| `sheets.py` | Google Sheets テンプレート複製・書き込み | `copy_template()`, `write_data()` |
| `params_validator.py` | JSON パラメータのスキーマバリデーション | `validate_params()` |
| `batch_runner.py` | 複数設定ファイルの順次実行 | `run_batch()` |
| `job_manager.py` | 非同期ジョブキュー管理 | `submit_job()`, `get_status()`, `cancel_job()` |
| `result_inspector.py` | クエリ結果の集計・パイプライン処理 | `transform()`, `aggregate()`, `filter_data()` |
| `analysis.py` | データ分析ユーティリティ | `show()`, `properties()`, `sites()` |
| `notebook.py` | Notebook 初期化ヘルパー | `init()` |

### プロジェクト固有モジュール

| モジュール | 役割 | 用途 |
|---|---|---|
| `articles.py` | GA4 記事メタデータの集約（言語別タイトル、カテゴリ等） | SLQM コンテンツ分析 |
| `periods.py` | 期間トークン（`2025`, `2025Q1`, `0`）のパース | SLQM 期間指定 |
| `slqm_analysis.py` | SLQM専用の分析ヘルパー関数（日別指標、チャネル分析等） | SLQM データ分析 |
| `talks_retention.py` | Talks リテンション分析（BigQuery コホート） | Talks 定着分析 |
| `dei_ga4.py` | DEI サイト分析ヘルパー | DEI レポート |
| `with_report.py` | WITH サイトレポート生成 | WITH 月次レポート |

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
