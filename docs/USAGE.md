# 使い方ガイド

GA4 / Search Console / BigQuery のデータを取得・加工・可視化するためのツールキット。
3つのインターフェースから同じデータにアクセスできる。

```
              GA4 / GSC / BigQuery
                     |
              megaton ライブラリ
                     |
    +----------------+----------------+
    v                v                v
 Notebook          CLI            Streamlit
 対話的分析      バッチ/自動化     ブラウザUI
```

| インターフェース | 向いている用途 |
|-----------------|--------------|
| **Notebook** | 探索的な分析、可視化、レポート開発 |
| **CLI** | 定期実行、バッチ処理、AI Agent からの利用 |
| **Streamlit UI** | パラメータを調整しながらの対話的分析 |

---

## セットアップ

### 必要なもの

- Python 3.10+
- GCP サービスアカウント JSON（GA4 / GSC / BQ の API アクセス権限付き）

### インストール

```bash
pip install -r requirements.txt

# credentials/ にサービスアカウント JSON を配置
# 複数ファイル可（property_id / site_url で自動ルーティング）
```

### 動作確認

```bash
# テストが通れば OK
python -m pytest -q

# GA4 プロパティ一覧が取れれば認証も OK
python scripts/query.py --list-ga4-properties
```

---

## クイックスタート

### Streamlit UI で試す

```bash
streamlit run app/streamlit_app.py
# → http://localhost:8501
```

ブラウザでデータソース（GA4 / GSC / BQ）を選び、プロパティと日付を指定して「実行」。

### CLI で GA4 データを取得する

1. パラメータ JSON を用意:

```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "site": "corp",
  "date_range": { "start": "today-7d", "end": "today" },
  "dimensions": ["date"],
  "metrics": ["sessions", "activeUsers"]
}
```

`site` は `configs/sites.json` の alias（`corp`, `shibuya` など）。`property_id` を直接指定してもよい。

2. 実行:

```bash
python scripts/query.py --params output/ga4_quickstart.json --json
```

`--json` 実行時は machine-readable な JSON のみを出力し、警告メッセージは `data.warnings` に格納される。

### Notebook で分析する

```python
# %% tags=["parameters"]
PROPERTY_ID = "123456789"
START_DATE = "2026-01-01"
END_DATE = "2026-01-31"

from megaton_lib.notebook import init; init()  # noqa: E702
from megaton_lib.megaton_client import get_ga4
from megaton_lib.analysis import show

# %% データ取得
mg = get_ga4(PROPERTY_ID)
mg.report.set.dates(START_DATE, END_DATE)
result = mg.report.run(
    d=["date", "sessionDefaultChannelGroup"],
    m=["sessions", "activeUsers"],
    show=False
)
show(result.df)
```

---

## やりたいこと別レシピ

### GA4 のトラフィック推移を見る

```python
mg = get_ga4("PROPERTY_ID")
mg.report.set.dates("2026-01-01", "2026-01-31")
result = mg.report.run(
    d=["date"], m=["sessions", "activeUsers"], show=False
)
result.sort("date")
show(result.df)
```

### チャネル別に絞り込む

```python
result = mg.report.run(
    d=["date"], m=["sessions"],
    filter_d="sessionDefaultChannelGroup==Organic Search",
    show=False
)
```

### GA4 スパイクを期間比較で調べる（期間A vs 期間B）

1. 分解データを取得:

```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "site": "corp",
  "date_range": {"start": "2026-01-01", "end": "2026-01-12"},
  "dimensions": ["date", "landingPage", "sessionDefaultChannelGroup"],
  "metrics": ["sessions", "activeUsers"],
  "filter_d": "landingPage=@/jp/company/",
  "limit": 50000,
  "pipeline": {"sort": "date ASC"}
}
```

2. 実行して CSV 化:

```bash
python scripts/query.py --params output/corp_company_spike.json --output output/corp_company_spike.csv
```

3. 比較期間を決めて増分を算出:
- 例: 基準期間 `2026-01-01`〜`2026-01-05`
- 例: スパイク期間 `2026-01-06`〜`2026-01-08`
- `landingPage` / `sessionDefaultChannelGroup` / その掛け合わせで `sessions` 増分を比較

注: 期間差分（delta）の自動算出は CLI 単体機能としては未実装。`--output` で保存した CSV を pandas / Notebook / BI ツールで比較する。

### GSC の検索クエリを分析する

```python
mg = get_gsc("https://example.com/")
mg.search.set.dates("2026-01-01", "2026-01-31")
result = mg.search.run(dimensions=["query", "page"], limit=25000)
result.decode().clean_url().filter_impressions(min=10)
show(result.df)
```

### 結果を CSV に保存する

CLI の場合:
```bash
python scripts/query.py --params input/params.json --output output/result.csv
```

Notebook の場合:
```python
show(result.df, save="output/result.csv")
```

### 結果を Google Sheets に保存する

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
mg.save.to.sheet("シート名", result.df, sort_by="date")
```

### BigQuery で SQL を実行する

CLI の場合（params.json）:
```json
{
  "schema_version": "1.0",
  "source": "bigquery",
  "project_id": "my-gcp-project",
  "sql": "SELECT event_date, COUNT(*) as cnt FROM `project.dataset.events_*` GROUP BY 1"
}
```

Notebook の場合:
```python
from megaton_lib.megaton_client import query_bq
df = query_bq("my-gcp-project", "SELECT ...", location="asia-northeast1")
```

### CLI で結果をフィルタ・集計する

```bash
# 同期実行 → params.json の pipeline で指定
python scripts/query.py --params input/params.json --json

# ジョブ結果に対して → CLI 引数で指定
python scripts/query.py --result <job_id> --where "clicks > 10" --sort "clicks DESC" --head 20
```

パイプラインの詳細は [REFERENCE.md](REFERENCE.md#result-pipeline) を参照。

---

## Notebook の詳しい使い方

### セル構成

パラメータセルと本処理セルの2段階構成を推奨:

#### セル1: パラメータとセットアップ

```python
# %% tags=["parameters"]
PROPERTY_ID = "123456789"
START_DATE = "2026-01-01"
END_DATE = "2026-01-31"
OUTPUT_DIR = "output"

import sys; sys.path.insert(0, "..")  # noqa: E702  ← reports/ 等サブディレクトリ時のみ
from megaton_lib.notebook import init; init()  # noqa: E702

import pandas as pd
from megaton_lib.megaton_client import get_ga4, get_gsc
from megaton_lib.analysis import show
```

- `tags=["parameters"]` により、CLI 実行時に `-p START_DATE=today-7d` でパラメータを上書き可能
- `init()` がパス解決・環境変数設定・モジュールリロードを一括実行
- `notebooks/` 直下なら `sys.path.insert` は不要

#### セル2以降: 本処理

```python
# %% データ取得
mg = get_ga4(PROPERTY_ID)
mg.report.set.dates(START_DATE, END_DATE)
result = mg.report.run(
    d=["date", "sessionDefaultChannelGroup"],
    m=["sessions", "totalUsers"],
    show=False
)
df = result.df

# %% 可視化
import matplotlib.pyplot as plt
df.pivot_table(index="date", columns="sessionDefaultChannelGroup",
               values="sessions", aggfunc="sum").plot(figsize=(12, 6))
plt.savefig(f"{OUTPUT_DIR}/trend.png")
show(df, save=f"{OUTPUT_DIR}/trend.csv")
```

### CLI からノートブックを実行

Jupyter で開発したノートブックをそのままバッチ実行:

```bash
python scripts/run_notebook.py notebooks/reports/my_report.py

# パラメータ上書き（日付テンプレート対応）
python scripts/run_notebook.py notebooks/reports/my_report.py \
  -p START_DATE=today-30d -p END_DATE=today
```

### Jupytext（.ipynb と .py の同期）

AI Agent がノートブックを直接編集すると壊れやすいため、Jupytext で `.py` ファイルを正とする:

1. `.py` ファイルを編集
2. `jupytext --sync notebooks/**/*.ipynb` で同期
3. Jupyter で `.ipynb` を開いて実行

---

## CLI の詳しい使い方

`scripts/query.py` がデータソースを自動判別して GA4 / GSC / BigQuery を実行する。

### 基本コマンド

```bash
# params.json で同期実行
python scripts/query.py --params input/params.json

# JSON 出力（AI Agent 向け）
python scripts/query.py --params input/params.json --json

# CSV 保存
python scripts/query.py --params input/params.json --output output/result.csv
```

### 非同期ジョブ

大量データの取得はジョブとして非同期実行できる:

```bash
python scripts/query.py --submit --params input/params.json
python scripts/query.py --status <job_id>
python scripts/query.py --result <job_id> --head 20
```

### バッチ実行

```bash
python scripts/query.py --batch configs/weekly/ --json
```

ディレクトリ内の JSON をファイル名順に一括実行。1つが失敗しても残りは続行。

### 一覧取得

```bash
python scripts/query.py --list-ga4-properties
python scripts/query.py --list-gsc-sites
python scripts/query.py --list-bq-datasets --project my-project
```

全オプションの一覧は [REFERENCE.md](REFERENCE.md#options) を参照。

---

## Streamlit UI の詳しい使い方

### 起動

```bash
streamlit run app/streamlit_app.py
# → http://localhost:8501
```

### 主な機能

- データソース選択（GA4 / GSC / BigQuery）
- プロパティ / サイトの動的取得・選択
- 日付範囲、フィルタ、集計の設定
- テーブル / チャート表示（折れ線 / 棒グラフ）
- CSV 保存 / ダウンロード
- 日英切り替え

### AI Agent との連携

Cursor、Claude Code、GitHub Copilot 等の AI Agent と連携して分析できる。

#### 仕組み

```
ユーザー: 「直近7日間のOrganic Search推移を見せて」
  ↓
Agent が input/params.json を書き込み（スキーマ検証済み）
  ↓
Streamlit UI が 2 秒ごとに監視し、変更を自動反映
  ↓
「自動実行」ON なら、そのままクエリ実行 → 結果表示
  ↓
Agent が output/result_*.csv を読んで分析続行
```

#### Agent の 2 つの経路

| 経路 | 特徴 | 使い分け |
|------|------|---------|
| **UI 経由** | `params.json` → Streamlit が自動反映 | 人間がパラメータを確認・修正したい場合 |
| **CLI 直接** | `scripts/query.py --params ... --json` | 確認不要で高速に結果が欲しい場合 |

#### サイドバーの設定

- **JSON 自動反映**: ファイル監視の ON/OFF
- **自動実行**: パラメータ反映後に自動でクエリ実行
- **JSON を開く**: 手動で params.json を読み込み

#### Agent 向けプロジェクト設定

- **AGENTS.md**: プロジェクトルートに配置。Cursor / Claude Code / Codex が自動認識し、ディレクトリ構成・コマンド・ルールを把握する
- **params.json スキーマ**: `schemas/query-params.schema.json` で定義。Agent が不正な JSON を書いても実行前にエラーで弾かれる

#### MCP との違い

MCP（Model Context Protocol）は Agent が API tool を直接呼び出すプロトコル。megaton-app はファイルベースのアプローチを採用している。

| | megaton-app | MCP |
|---|---|---|
| **連携方式** | Agent が JSON ファイルを書く | Agent が MCP サーバーの tool を呼ぶ |
| **コンテキスト消費** | AGENTS.md のみ（数百トークン） | tool 定義 + リクエスト + レスポンスが毎回載る |
| **セットアップ** | 不要（ファイルを書くだけ） | MCP サーバーの起動・設定が必要 |
| **人間の介入** | UI でパラメータを確認・修正できる | Agent が直接実行（介入しにくい） |
| **Agent 依存** | なし（ファイルを書ければ何でも可） | MCP クライアント対応が必要 |
| **結果の扱い** | CSV ファイル（必要な部分だけ読める） | レスポンス全体がコンテキストに載る |

---

## テスト

```bash
# 全テスト
python -m pytest -q

# レイヤ別
python -m pytest -q -m unit
python -m pytest -q -m integration

# query.py のカバレッジ
python -m pytest -q --cov=scripts.query --cov-report=term-missing --cov-fail-under=90
```

---

## 詳細リファレンス

| ドキュメント | 内容 |
|------------|------|
| [REFERENCE.md](REFERENCE.md) | JSON スキーマ、CLI 全オプション、パイプライン、megaton API、認証 |
| [CHANGELOG.md](CHANGELOG.md) | 変更履歴 |
