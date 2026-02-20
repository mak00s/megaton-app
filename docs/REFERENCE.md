# 技術リファレンス

## Streamlit UI アーキテクチャ

人間とAI Agentが協力してデータ分析を行うフロー：

```
┌─────────────────────────────────────────────────────────────────┐
│                        人間 + AI Agent                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [1] 人間: 自然言語で要求                                        │
│      「直近7日間のOrganic Search推移を見たい」                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [2] AI Agent: input/params.json にパラメータを書き込む          │
│      {                                                          │
│        "schema_version": "1.0",                                 │
│        "source": "ga4",                                         │
│        "property_id": "254470346",                              │
│        "date_range": {"start": "...", "end": "..."},            │
│        "dimensions": ["date"],                                  │
│        "metrics": ["sessions"],                                 │
│        "filter_d": "sessionDefaultChannelGroup==Organic Search" │
│      }                                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [3] Streamlit UI: 自動反映（2秒ごとにファイル監視）             │
│      - 更新時刻 + 実質差分で変更判定（空白/インデント/キー順を無視）│
│      - UIに自動でパラメータが反映される                          │
│      - 人間がドロップダウンで日付・プロパティを微調整             │
│      - 「自動実行」ONなら自動でクエリ実行                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [4] Streamlit UI: 結果表示                                     │
│      - テーブル / チャート（折れ線/棒グラフ）                     │
│      - CSV保存 → output/result_*.csv                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [5] 人間: 結果確認 → OK / 修正依頼                              │
│      OK → AI Agent が output/*.csv を読んで分析続行              │
│      NG → [1] に戻る                                            │
└─────────────────────────────────────────────────────────────────┘
```

### 役割分担

| 役割 | 人間 | AI Agent |
|------|------|----------|
| 要求定義 | 自然言語で指示 | - |
| パラメータ生成 | - | JSONを生成 |
| パラメータ確認 | UIで確認・修正 | - |
| 実行 | ボタン押下 or 自動 | - |
| 結果確認 | 目視 | - |
| 分析 | 判断 | データ処理・考察 |

### 実際のユースケース

#### ユースケース1: 流入チャネル分析

**人間の要求:**
> 「先月のOrganic Search流入の日別推移を見たい」

**AI Agentの処理:**
```python
# input/params.json を生成
{
  "schema_version": "1.0",
  "source": "ga4",
  "property_id": "254800682",
  "date_range": {"start": "prev-month-start", "end": "prev-month-end"},
  "dimensions": ["date"],
  "metrics": ["sessions", "totalUsers"],
  "filter_d": "sessionDefaultChannelGroup==Organic Search"
}
```

**人間の確認（Streamlit UI）:**
- 日付範囲を確認: 2026-01-01 〜 2026-01-31 ✓
- フィルタ条件を確認: Organic Search のみ ✓
- 実行ボタンクリック

**結果:**
- UIにグラフとテーブルが表示
- CSV保存 → AI Agentが読み込んで次の分析へ

#### ユースケース2: BigQueryコホート分析

**人間の要求:**
> 「2月に初訪問したユーザーの7日後定着率を調べたい」

**AI Agentの処理:**
```python
# input/params.json を生成
{
  "schema_version": "1.0",
  "source": "bigquery",
  "project_id": "my-project",
  "query": """
    SELECT
      cohort_date,
      COUNT(DISTINCT user_pseudo_id) as cohort_size,
      COUNT(DISTINCT CASE WHEN days_since = 7 THEN user_pseudo_id END) as day7_retained
    FROM cohort_table
    WHERE cohort_date >= '2026-02-01'
    GROUP BY cohort_date
  """,
  "location": "asia-northeast1"
}
```

**人間の確認:**
- SQLを確認してWHERE句の日付を微調整
- 実行してコホート定着率を確認

#### ユースケース3: Search Console検索クエリ分析

**人間の要求:**
> 「ブログページの検索クエリTop20（クリック数順）」

**AI Agentの処理:**
```python
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.example.com/",
  "date_range": {"start": "today-30d", "end": "today-3d"},
  "dimensions": ["query", "page"],
  "filter": "page:includingRegex:/blog/",
  "limit": 25000,
  "pipeline": {
    "sort": "clicks DESC",
    "head": 20
  }
}
```

**人間の確認:**
- 日付範囲とフィルタを確認
- Top20のクエリを確認してコンテンツ戦略を判断

### データフロー図解

#### パターンA: AI Agent直接実行（CLIのみ、人間の確認なし）

```
┌─────────────┐
│ AI Agent    │ 自然言語要求を受け取り
└──────┬──────┘
       │
       │ 1. params.json生成
       ▼
┌─────────────────────────┐
│ input/params.json       │
│ {source, date_range...} │
└──────┬──────────────────┘
       │
       │ 2. CLI実行
       ▼
┌──────────────────────────┐
│ scripts/query.py         │
│ --params input/params.json│
└──────┬───────────────────┘
       │
       │ 3. API呼び出し
       ▼
┌─────────────┐
│ GA4/GSC/BQ  │ データソース
└──────┬──────┘
       │
       │ 4. 結果保存
       ▼
┌──────────────────┐
│ output/result.csv│
└──────┬───────────┘
       │
       │ 5. 分析続行
       ▼
┌─────────────┐
│ AI Agent    │ 結果を読んで考察
└─────────────┘
```

**使いどころ:** 定期レポート、バッチ処理、探索的分析（人間の確認不要）

#### パターンB: Streamlit UI連携（人間の確認あり）

```
┌─────────────┐
│ AI Agent    │ 自然言語要求を受け取り
└──────┬──────┘
       │
       │ 1. params.json生成
       ▼
┌─────────────────────────┐
│ input/params.json       │
│ {source, date_range...} │
└──────┬──────────────────┘
       │
       │ 2. ファイル監視（2秒ごと）
       ▼
┌─────────────────────────┐
│ Streamlit UI            │
│ パラメータ自動反映       │
└──────┬──────────────────┘
       │
       │ 3. 人間が確認・修正
       ▼
┌─────────────┐
│   人間      │ UIで日付・フィルタ調整
└──────┬──────┘
       │
       │ 4. 実行ボタン or 自動実行
       ▼
┌──────────────────────────┐
│ Streamlit UI             │
│ クエリ実行 → 結果表示     │
└──────┬───────────────────┘
       │
       │ 5. CSV保存
       ▼
┌──────────────────┐
│ output/result.csv│
└──────┬───────────┘
       │
       │ 6. 結果確認 → OK/NG判断
       ▼
┌─────────────┐
│   人間      │ 目視確認
└──────┬──────┘
       │
       │ OK → 7. 分析続行
       ▼
┌─────────────┐
│ AI Agent    │ 結果を読んで考察
└─────────────┘
```

**使いどころ:** 探索的分析（パラメータの妥当性確認が必要）、アドホック分析

### サンプルコード: Pythonでの直接実行

```python
# AI Agent が探索的分析を行う場合（Streamlit/CLI不使用）
from megaton_lib.megaton_client import query_ga4, query_gsc, query_bq
from megaton_lib.analysis import show, properties, sites

# 1. 利用可能なプロパティ/サイトを確認
properties()  # GA4プロパティ一覧
sites()       # GSCサイト一覧

# 2. クエリ実行（結果はDataFrame、contextに載らない）
df_ga4 = query_ga4(
    "254800682",
    "2025-06-01",
    "2026-01-31",
    dimensions=["date", "sessionDefaultChannelGroup"],
    metrics=["sessions", "totalUsers"],
    filter_d="sessionDefaultChannelGroup==Organic Search"
)

df_gsc = query_gsc(
    "https://www.example.com/",
    "2026-01-01",
    "2026-01-31",
    dimensions=["query", "page"],
    filter="page:includingRegex:/blog/",
    limit=25000
)

# 3. Python加工（contextゼロ）
df_ga4_pivot = df_ga4.pivot_table(
    index="date",
    columns="sessionDefaultChannelGroup",
    values="sessions",
    aggfunc="sum"
)

df_gsc_top = df_gsc.sort_values("clicks", ascending=False).head(20)

# 4. 表示（行数制限付き、必要ならCSV保存）
show(df_ga4_pivot, n=10, save="output/ga4_trend.csv")
show(df_gsc_top, n=20, save="output/gsc_top_queries.csv")

# 5. BigQueryパラメータ化クエリ
df_cohort = query_bq(
    project_id="my-project",
    query="""
        SELECT cohort_date, COUNT(DISTINCT user_pseudo_id) as cohort_size
        FROM cohort_table
        WHERE cohort_date >= @start_date
        GROUP BY cohort_date
    """,
    params={"start_date": "2026-02-01"},
    location="asia-northeast1"
)
show(df_cohort, save="output/cohort.csv")
```

**ルール:**
- `print(df.to_string())` は禁止。常に `show()` を使う
- 大きい結果は `save=` でCSV保存し、contextにはサマリだけ載せる
- 加工・集計はpandas で行い、最終結果だけ `show()` する

---

## JSONパラメータスキーマ

- スキーマファイル: `schemas/query-params.schema.json`
- `schema_version` は必須（現在は `"1.0"`）
- `source` ごとに許可されるキー以外はエラー（`additionalProperties: false`）
- Streamlit と CLI（`scripts/query.py --params ...`）で同じスキーマを共通利用

## CLI Job管理

### コマンド

```bash
# 非同期投入
python scripts/query.py --submit --params input/params.json

# 状態確認
python scripts/query.py --status <job_id>

# キャンセル
python scripts/query.py --cancel <job_id>

# 結果確認
python scripts/query.py --result <job_id>

# 結果の先頭N行
python scripts/query.py --result <job_id> --head 20

# 結果の要約統計
python scripts/query.py --result <job_id> --summary

# 結果の変換（`--result`）
python scripts/query.py --result <job_id> --json --transform "date:date_format"
python scripts/query.py --result <job_id> --json --transform "page:url_decode,page:strip_qs,page:path_only"
python scripts/query.py --result <job_id> --json --transform "page:strip_qs:id,ref" --group-by "page" --aggregate "sum:clicks"

# 結果のパイプライン処理（`--result`）
python scripts/query.py --result <job_id> --json --where "impressions >= 100 and ctr < 0.02" --sort "impressions DESC" --columns "query,clicks,impressions" --head 20
python scripts/query.py --result <job_id> --json --group-by "page" --aggregate "sum:clicks,mean:ctr" --sort "sum_clicks DESC"

# 同期実行のパイプライン処理（params.json の pipeline フィールドで指定）
python scripts/query.py --params input/params.json --json

# ジョブ一覧
python scripts/query.py --list-jobs
```

### `--json` レスポンス形式

成功時:

```json
{
  "status": "ok",
  "mode": "query",
  "data": {}
}
```

補足:
- 同期実行（`--params`）で params.json に `pipeline` フィールドが含まれる場合、`data.pipeline` に `input_rows` / `output_rows` を含む実行メタが入る。
- `--params` 同期実行時は CLI パイプライン引数（`--where` 等）は使用不可。`pipeline` は params.json 内で指定する。
- `--result` ジョブ結果時は従来通り CLI 引数（`--where` / `--sort` 等）で指定する。

失敗時:

```json
{
  "status": "error",
  "error_code": "PARAMS_VALIDATION_FAILED",
  "message": "Params validation failed.",
  "hint": "Fix params based on details[].",
  "details": {}
}
```

### 結果パイプラインオプション

`--result`（ジョブ結果CSV）または同期実行（`--params`）の結果DataFrameに対して変換し、必要行だけ返す。

処理順序（固定）:
`CSV読み込み → transform → where → group-by+aggregate → sort → columns → head → 出力`

#### オプション

| オプション | 書式 | 説明 |
|-----------|------|------|
| `--transform` | `col:func,col2:func2` | 列変換 |
| `--where` | pandas query式 | 行フィルタ |
| `--sort` | `col DESC,col2 ASC` | ソート |
| `--columns` | `col1,col2` | 列選択 |
| `--group-by` | `col1,col2` | グループ列 |
| `--aggregate` | `sum:clicks,mean:ctr` | 集計 |
| `--head` | `N` | 先頭N行 |

#### 変換関数（`--transform`）

| 関数 | 書式 | 説明 |
|------|------|------|
| `date_format` | `date:date_format` | YYYYMMDD → YYYY-MM-DD |
| `url_decode` | `page:url_decode` | %エンコード解除 |
| `path_only` | `page:path_only` | URLからパスのみ抽出（ドメイン除去） |
| `strip_qs` | `page:strip_qs` | 全クエリパラメータ除去 |
| `strip_qs` | `page:strip_qs:id,ref` | 指定パラメータのみ保持（他は除去） |

**strip_qs の引数仕様:**
- 引数なし（`page:strip_qs`）→ 全クエリパラメータとフラグメントを除去
- 引数あり（`page:strip_qs:id,ref`）→ 指定パラメータのみ保持、他は除去

**カンマ曖昧性の解決:**
`page:strip_qs:id,ref` の `ref` はstrip_qsの引数（新しいtransformではない）。コロンを含まないセグメントは直前のtransformの引数に追加される。

#### 集計関数

`sum`, `mean`, `count`, `min`, `max`, `median`

#### 制約

- `--result` 時: CLI引数 `--transform` / `--where` / `--sort` / `--columns` / `--group-by` / `--aggregate` / `--head` で指定
- `--params` 同期実行時: params.json の `pipeline` フィールドで指定。CLI引数は使用不可
- `--group-by` と `--aggregate` は同時指定必須
- `--summary` とパイプラインオプションは排他

#### エラーコード

| code | 条件 |
|------|------|
| `INVALID_TRANSFORM` | `--transform` 関数/列/式が不正 |
| `INVALID_WHERE` | `--where` 式が不正 |
| `INVALID_SORT` | `--sort` 書式不正 / 列不正 |
| `INVALID_COLUMNS` | `--columns` に存在しない列 |
| `INVALID_AGGREGATE` | 集計関数/列/式が不正 |

### ジョブ状態

| status | 説明 |
|--------|------|
| `queued` | キュー投入済み |
| `running` | 実行中 |
| `canceled` | キャンセル済み |
| `succeeded` | 成功（`artifact_path` に結果CSVあり） |
| `failed` | 失敗（`error` に詳細あり） |

### 保存構造

| パス | 内容 |
|------|------|
| `output/jobs/records/<job_id>.json` | ジョブメタデータ |
| `output/jobs/logs/<job_id>.log` | 実行ログ |
| `output/jobs/artifacts/<job_id>.csv` | 結果CSV |

### GA4クエリ

```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "property_id": "254470346",
  "date_range": {
    "start": "2026-01-28",
    "end": "2026-02-03"
  },
  "dimensions": ["date"],
  "metrics": ["sessions", "activeUsers"],
  "filter_d": "sessionDefaultChannelGroup==Organic Search",
  "limit": 1000
}
```

### GSCクエリ

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.example.com/",
  "date_range": {
    "start": "2026-01-21",
    "end": "2026-02-03"
  },
  "dimensions": ["query"],
  "filter": "query:contains:渋谷",
  "limit": 1000
}
```

### BigQueryクエリ

```json
{
  "schema_version": "1.0",
  "source": "bigquery",
  "project_id": "my-gcp-project",
  "sql": "SELECT event_date, COUNT(*) as cnt FROM `project.dataset.events_*` GROUP BY 1"
}
```

### フィールド説明

| フィールド | 型 | 必須 | 説明 |
|-----------|-----|------|------|
| `schema_version` | string | ✓ | 現在は `"1.0"` 固定 |
| `source` | string | ✓ | `"ga4"`, `"gsc"`, `"bigquery"` |
| `property_id` | string | GA4時 | GA4プロパティID |
| `site_url` | string | GSC時 | Search ConsoleサイトURL |
| `project_id` | string | BQ時 | GCPプロジェクトID |
| `sql` | string | BQ時 | 実行するSQL |
| `date_range.start` | string | GA4/GSC | 開始日（YYYY-MM-DD or テンプレート） |
| `date_range.end` | string | GA4/GSC | 終了日（YYYY-MM-DD or テンプレート） |
| `dimensions` | array | - | ディメンション一覧 |
| `metrics` | array | GA4時 | メトリクス一覧 |
| `filter_d` | string | GA4時 | GA4フィルタ（`field==value`形式） |
| `filter` | string | GSC時 | GSCフィルタ（`dim:op:expr`形式） |
| `limit` | number | - | 結果件数上限（最大10万） |
| `pipeline` | object | - | 取得後のパイプライン処理（下記参照） |

#### pipeline フィールド

`pipeline` は取得結果に対する後処理を定義するオブジェクト。全 source で使用可能。

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `pipeline.transform` | string | 列変換（`col:func` 形式） |
| `pipeline.where` | string | 行フィルタ（pandas query式） |
| `pipeline.sort` | string | ソート（`col DESC,col2 ASC`） |
| `pipeline.columns` | string | 列選択（`col1,col2`） |
| `pipeline.group_by` | string | グループ列（`col1,col2`） |
| `pipeline.aggregate` | string | 集計（`sum:clicks,mean:ctr`） |
| `pipeline.head` | integer | 先頭N行 |

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.example.com/",
  "date_range": {"start": "2026-01-21", "end": "2026-02-03"},
  "dimensions": ["query", "page"],
  "limit": 25000,
  "pipeline": {
    "transform": "page:url_decode,page:strip_qs,page:path_only",
    "where": "clicks > 10",
    "group_by": "page",
    "aggregate": "sum:clicks,sum:impressions",
    "sort": "sum_clicks DESC",
    "head": 50
  }
}
```

### save フィールド

`save` はクエリ結果（pipeline適用後）の保存先を定義するオブジェクト。全 source で使用可能。

| フィールド | 型 | 必須 | 説明 |
|-----------|-----|------|------|
| `save.to` | string | ○ | 保存先: `csv`, `sheets`, `bigquery` |
| `save.mode` | string | | 保存モード: `overwrite`(デフォルト), `append`, `upsert` |
| `save.path` | string | CSV時○ | ファイルパス（例: `output/report.csv`） |
| `save.sheet_url` | string | Sheets時○ | スプレッドシートURL |
| `save.sheet_name` | string | | シート名（デフォルト: `data`） |
| `save.project_id` | string | BQ時○ | GCPプロジェクトID |
| `save.dataset` | string | BQ時○ | データセットID |
| `save.table` | string | BQ時○ | テーブルID |
| `save.keys` | string[] | upsert時○ | アップサートのキー列 |

**モード制約:**

| モード | CSV | Sheets | BigQuery |
|--------|-----|--------|----------|
| overwrite | ○ | ○ | ○ |
| append | ○ | ○ | ○ |
| upsert | × | ○ | ×（将来対応） |

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.example.com/",
  "date_range": {"start": "2026-01-21", "end": "2026-02-03"},
  "dimensions": ["query", "page"],
  "limit": 25000,
  "pipeline": {
    "transform": "page:url_decode,page:strip_qs,page:path_only",
    "group_by": "page",
    "aggregate": "sum:clicks,sum:impressions",
    "sort": "sum_clicks DESC"
  },
  "save": {
    "to": "bigquery",
    "project_id": "my-project",
    "dataset": "analytics",
    "table": "gsc_pages",
    "mode": "overwrite"
  }
}
```

#### 日付テンプレート

`date_range.start` / `date_range.end` には絶対日付の他にテンプレート式が使える。
バリデーション時に実日付に解決される。

| テンプレート式 | 意味 |
|--------------|------|
| `today` | 実行日 |
| `today-Nd` | N日前（例: `today-7d`） |
| `today+Nd` | N日後（例: `today+3d`） |
| `month-start` | 当月1日 |
| `month-end` | 当月末日 |
| `prev-month-start` | 前月1日 |
| `prev-month-end` | 前月末日 |
| `week-start` | 今週月曜日 |
| `YYYY-MM-DD` | 絶対日付（パススルー） |

#### バッチ実行

`--batch <dir>` でディレクトリ内のJSONをファイル名順に一括実行する。

```bash
python scripts/query.py --batch configs/weekly/ --json
```

各configは独立した1ステップとして順番に実行される。
1つが失敗しても残りは続行。最後にサマリを出力。

JSON出力例:
```json
{
  "status": "ok",
  "total": 3,
  "succeeded": 2,
  "failed": 1,
  "skipped": 0,
  "results": [
    {"config": "01_gsc.json", "status": "ok", "row_count": 500},
    {"config": "02_ga4.json", "status": "ok", "row_count": 120},
    {"config": "03_bq.json", "status": "error", "error": "..."}
  ],
  "elapsed_sec": 12.34
}
```

#### ノートブック実行（CLI）

`scripts/run_notebook.py` で Jupytext percent format の `.py` ノートブックを CLI から実行する。
Jupyter で対話的に開発したノートブックを、そのまま CLI / GitHub Actions で定期実行できる。

```bash
python scripts/run_notebook.py <notebook.py> [-p KEY=VALUE ...]
```

| オプション | 説明 |
|-----------|------|
| `<notebook.py>` | 実行する `.py` ノートブック |
| `-p KEY=VALUE` | パラメータ上書き（複数指定可、日付テンプレート対応） |

**パラメータ上書きの仕組み:**

ノートブック内の `# %% tags=["parameters"]` セルの変数が上書き対象。

```python
# %% tags=["parameters"]
START_DATE = "2025-06-01"    # ← -p START_DATE=today-30d で上書き可
END_DATE = "2026-01-31"      # ← -p END_DATE=today で上書き可
OUTPUT_DIR = "../output/yokohama"
```

- 日付テンプレート（`today`, `today-30d` 等）は自動的に実日付に解決
- 数値はそのまま、文字列は引用符付きで代入
- `MPLBACKEND=Agg` で実行するため、`plt.show()` は呼ばれてもGUI表示されない

**ノートブック作成規約:**

1. `# %% tags=["parameters"]` セルに外から変更したい変数を集約
2. セットアップセルで `from setup import init; init()` を呼ぶ
3. 変数名は `UPPER_SNAKE_CASE`

---

## 認証情報

### サービスアカウント JSON

- 格納場所: `credentials/` ディレクトリ
- Git管理: **除外**（.gitignoreで設定済み）
- 推奨指定: 環境変数 `MEGATON_CREDS_PATH`（JSONファイル or JSONを1つ含むディレクトリ）

### 認証解決ルール（実装準拠）

#### GA4 / GSC（Megaton経由）

`MEGATON_CREDS_PATH` と `credentials/*.json` から候補を収集し、
`get_ga4(property_id)` / `get_gsc(site_url)` が対象に対応する認証を選ぶ。

候補解決順:
1. `MEGATON_CREDS_PATH` がファイルを指す: その1件
2. `MEGATON_CREDS_PATH` がディレクトリを指す: 配下 `*.json`（ファイル名昇順）
3. 未指定時: `credentials/*.json`（親ディレクトリ探索あり）

#### BigQuery（native client）

`query_bq(..., params=...)` は `google.cloud.bigquery.Client` を使用。
認証解決順:
1. `GOOGLE_APPLICATION_CREDENTIALS` が設定済みならそれを優先
2. 未設定時、`MEGATON_CREDS_PATH` / `credentials/*.json` 候補から1件選択
   - `get_bq_client(project_id, creds_hint="corp")` の `creds_hint` を含むファイル名を優先
   - 一致がなければ先頭候補

補足:
- `get_bq_client` の native client キャッシュキーは現在 `project_id` 単位
  （同一プロジェクトで複数認証を切り替える用途は想定外）
- `talks_retention.init_bq_client()` は deprecated ラッパーで、
  実体は `megaton_client.get_bq_client()` に委譲

### Notebook での指定

```python
# セットアップセルで init() を呼ぶだけ（パス解決・環境変数・モジュールを一括初期化）
from setup import init; init()
```

`notebooks/setup.py` の `init()` がプロジェクトルートを `__file__` から解決し、
`MEGATON_CREDS_PATH` を `credentials/` に自動設定する。

### スクリプトでの指定

```python
import os
CREDS_PATH = os.environ["MEGATON_CREDS_PATH"]  # ファイル or ディレクトリを指定
```

### 解決結果の確認

実行時にどの認証が使われるかを確認するには:

```python
from megaton_lib.megaton_client import describe_auth_context

info = describe_auth_context(creds_hint="corp")
# info["resolved_bq_creds_path"], info["resolved_bq_source"] などを確認
```

---

## megaton API

### 初期化（Notebook 推奨）

```python
from megaton_lib.megaton_client import get_ga4, get_gsc

# クレデンシャル自動選択 + アカウント/プロパティ選択済み
mg = get_ga4("PROPERTY_ID")

# クレデンシャル自動選択 + サイト選択済み
mg = get_gsc("https://example.com/")
```

`get_ga4()` / `get_gsc()` は megaton インスタンスを返す。
`mg.report.run()` → `ReportResult`、`mg.search.run()` → `SearchResult` でメソッドチェーンが可能。

### 初期化（低レベル）

```python
from megaton import start
from megaton_lib.credentials import resolve_service_account_path
mg = start.Megaton(resolve_service_account_path(), headless=True)
```

### GA4

```python
# get_ga4() を使う場合（推奨）
mg = get_ga4("PROPERTY_ID")
mg.report.set.dates("2026-01-01", "2026-01-31")
result = mg.report.run(d=["date"], m=["sessions"], filter_d="...", show=False)
result.clean_url("landingPage").group("date").sort("date")
df = result.df

# 低レベル API
mg.ga["4"].accounts  # [{"id": "...", "name": "...", "properties": [...]}]
mg.ga["4"].account.select("ACCOUNT_ID")
mg.ga["4"].property.select("PROPERTY_ID")
mg.report.set.dates("2026-01-01", "2026-01-31")
result = mg.report.run(d=["date"], m=["sessions"], filter_d="...", show=False)
df = result.df
```

#### ReportResult メソッド

| メソッド | 説明 |
|---------|------|
| `.clean_url(dim)` | URL正規化（デコード、クエリ除去、小文字化） |
| `.group(by, method='sum')` | グループ集計 |
| `.sort(by, ascending=True)` | ソート |
| `.fill(to='(not set)')` | 欠損値埋め |
| `.to_int(metrics)` | メトリクスを整数化 |
| `.replace(dim, by)` | 値の置換 |
| `.normalize(dim, by)` | 値の正規化（上書き） |
| `.categorize(dim, by, into)` | カテゴリ列の追加 |
| `.classify(dim, by)` | 正規化 + 集計 |
| `.df` | 最終 DataFrame を取得 |

#### dimensions / metrics の指定方法

```python
# 基本: 文字列のリスト
d = ["date", "sessionDefaultChannelGroup"]
m = ["sessions", "activeUsers"]

# 列名を変更: タプルで (API名, 表示名) を指定
d = [
    "date",
    ("sessionDefaultChannelGroup", "channel"),  # → 列名が "channel" になる
]
m = [
    ("sessions", "セッション"),      # → 列名が "セッション" になる
    ("activeUsers", "uu"),           # → 列名が "uu" になる
]

mg.report.run(d=d, m=m, show=False)
```

#### フィルタの書式

フィルタは**文字列**で指定する。書式: `<フィールド名><演算子><値>`

```python
# 単一フィルタ
mg.report.run(
    d=["date"], 
    m=["sessions"], 
    filter_d="sessionDefaultChannelGroup==Organic Search",
    show=False
)

# 複数フィルタはセミコロン(;)で区切る（AND条件）
mg.report.run(
    d=["date"], 
    m=["sessions"], 
    filter_d="sessionDefaultChannelGroup==Organic Search;country==Japan",
    show=False
)

# メトリクスのフィルタは filter_m を使用
mg.report.run(
    d=["date"], 
    m=["sessions"], 
    filter_m="sessions>100",
    show=False
)
```

**演算子一覧:**
| 演算子 | 説明 | 例 |
|-------|------|-----|
| `==` | 完全一致 | `country==Japan` |
| `!=` | 不一致 | `country!=Japan` |
| `=@` | 部分一致（contains） | `pagePath=@/blog/` |
| `!@` | 部分不一致 | `pagePath!@/admin/` |
| `=~` | 正規表現一致 | `pagePath=~^/products/` |
| `!~` | 正規表現不一致 | `pagePath!~/test/` |
| `>`, `>=`, `<`, `<=` | 数値比較 | `sessions>100` |

#### ソートの書式

ソートは文字列で指定。降順は先頭に `-` を付ける。

```python
# 日付で昇順ソート
mg.report.run(d=["date"], m=["sessions"], sort="date", show=False)

# セッション数で降順ソート
mg.report.run(d=["date"], m=["sessions"], sort="-sessions", show=False)

# 複数ソート（カンマ区切り）：日付昇順 → セッション降順
mg.report.run(d=["date", "country"], m=["sessions"], sort="date,-sessions", show=False)
```

#### よく使うディメンション・メトリクス

**ディメンション:**
| API名 | 説明 |
|-------|------|
| `date` | 日付 |
| `sessionDefaultChannelGroup` | チャネル（Organic Search等） |
| `sessionSource` | 参照元 |
| `sessionMedium` | メディア |
| `pagePath` | ページパス |
| `landingPage` | ランディングページ |
| `deviceCategory` | デバイス（desktop/mobile/tablet） |
| `country` | 国 |

**メトリクス:**
| API名 | 説明 |
|-------|------|
| `sessions` | セッション数 |
| `activeUsers` | アクティブユーザー数 |
| `newUsers` | 新規ユーザー数 |
| `screenPageViews` | ページビュー数 |
| `bounceRate` | 直帰率 |
| `averageSessionDuration` | 平均セッション時間 |
| `conversions` | コンバージョン数 |

### Search Console

```python
# get_gsc() を使う場合（推奨）
mg = get_gsc("https://example.com/")
mg.search.set.dates("2026-01-01", "2026-01-31")
result = mg.search.run(dimensions=["query", "page"], limit=25000)
result.decode().clean_url().normalize_queries().filter_impressions(min=10)
df = result.df

# 低レベル API
sites = mg.search.get.sites()
mg.search.use("https://example.com/")
mg.search.set.dates("2026-01-01", "2026-01-31")
mg.search.run(dimensions=["query"], metrics=["clicks", "impressions", "ctr", "position"])
df = mg.search.data

# フィルタ付きレポート
mg.search.run(
    dimensions=["query", "page"],
    dimension_filter=[
        {"dimension": "query", "operator": "contains", "expression": "渋谷"},
        {"dimension": "page", "operator": "includingRegex", "expression": "/blog/"}
    ]
)
```

#### SearchResult メソッド

| メソッド | 説明 |
|---------|------|
| `.decode()` | URL デコード（%xx → 文字） |
| `.clean_url(dim='page')` | URL正規化（デコード、クエリ除去、小文字化） |
| `.remove_params(keep=None)` | URLクエリパラメータ除去 |
| `.normalize_queries()` | クエリ空白の正規化・重複統合 |
| `.filter_clicks(min, max)` | クリック数でフィルタ |
| `.filter_impressions(min, max)` | 表示回数でフィルタ |
| `.filter_ctr(min, max)` | CTRでフィルタ |
| `.filter_position(min, max)` | 掲載順位でフィルタ |
| `.aggregate(by)` | 手動集計 |
| `.normalize(dim, by)` | 値の正規化（上書き） |
| `.categorize(dim, by, into)` | カテゴリ列の追加 |
| `.classify(dim, by)` | 正規化 + 集計 |
| `.apply_if(cond, method)` | 条件付きメソッド適用 |
| `.df` | 最終 DataFrame を取得 |

#### ディメンション・メトリクス

**ディメンション:**
| 名前 | 説明 |
|------|------|
| `query` | 検索クエリ |
| `page` | ページURL |
| `country` | 国 |
| `device` | デバイス（DESKTOP/MOBILE/TABLET） |
| `date` | 日付 |

**メトリクス:**
| 名前 | 説明 |
|------|------|
| `clicks` | クリック数 |
| `impressions` | 表示回数 |
| `ctr` | クリック率（0〜1） |
| `position` | 平均掲載順位 |

#### フィルタの書式

フィルタは辞書のリストで指定。

```python
dimension_filter = [
    {"dimension": "query", "operator": "contains", "expression": "渋谷"},
]
```

**演算子一覧:**
| 演算子 | 説明 |
|-------|------|
| `contains` | 部分一致 |
| `notContains` | 部分不一致 |
| `equals` | 完全一致 |
| `notEquals` | 不一致 |
| `includingRegex` | 正規表現一致 |
| `excludingRegex` | 正規表現不一致 |

### Google Sheets

#### スプレッドシートを開く

```python
# スプレッドシートを開く
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")

# シートを選択
mg.sheets.select("シート名")

# シート一覧
mg.sheets.list()

# シート作成・削除
mg.sheets.create("新規シート")
mg.sheets.delete("削除するシート")
```

#### データの読み込み

```python
# 現在のシートをDataFrameとして取得
df = mg.sheet.df()

# セル単位で読み込み
value = mg.load.cell(row=1, col=1)  # A1セル
```

#### データの保存（上書き）

```python
# シート名を指定して保存（シートがなければ作成）
mg.save.to.sheet("シート名", df)

# オプション
mg.save.to.sheet("シート名", df, 
    sort_by="date",        # ソート列
    sort_desc=True,        # 降順
    auto_width=True,       # 列幅自動調整
    freeze_header=True     # ヘッダー行を固定
)

# 現在選択中のシートに保存
mg.sheet.save(df)
```

#### データの追記

```python
# シート名を指定して追記（既存データの末尾に追加）
mg.append.to.sheet("シート名", df)

# 現在選択中のシートに追記
mg.sheet.append(df)
```

#### データのアップサート（マージ）

キー列を基準に、既存行は更新、新規行は追加。

```python
# シート名を指定してアップサート
mg.upsert.to.sheet("シート名", df, keys=["id"])

# 複数キー
mg.upsert.to.sheet("シート名", df, keys=["date", "channel"])

# オプション
mg.upsert.to.sheet("シート名", df, 
    keys=["id"],
    columns=["name", "value"],  # 更新する列を限定
    sort_by="id"                # ソート
)

# 現在選択中のシートにアップサート
mg.sheet.upsert(df, keys=["id"])
```

#### セル・範囲の書き込み

```python
# セル単位で書き込み
mg.sheet.cell.set("A1", "値")
mg.sheet.cell.set("B2", 123)

# 範囲に書き込み（2次元配列）
mg.sheet.range.set("A1:C3", [
    ["A1", "B1", "C1"],
    ["A2", "B2", "C2"],
    ["A3", "B3", "C3"],
])
```

#### シートのクリア

```python
# 現在のシートをクリア
mg.sheet.clear()
```

### BigQuery

#### 初期化

```python
# BigQueryサービスを起動（GCPプロジェクトIDを指定）
bq = mg.launch_bigquery("my-gcp-project")
```

#### SQLクエリの実行

```python
# DataFrameとして結果を取得
df = bq.run("SELECT * FROM `project.dataset.table` LIMIT 100", to_dataframe=True)

# イテレータとして取得（大量データ向け）
results = bq.run("SELECT * FROM `project.dataset.table`", to_dataframe=False)
for row in results:
    print(row)
```

#### データセット・テーブルの操作

```python
# データセット一覧
bq.datasets  # ['dataset1', 'dataset2', ...]

# データセットを選択
bq.dataset.select("my_dataset")

# テーブル一覧
bq.dataset.tables  # ['table1', 'table2', ...]

# テーブルを選択
bq.table.select("my_table")
```

#### GA4エクスポートテーブルの操作

```python
# GA4イベントデータの取得（日付範囲指定）
df = bq.ga4.events(
    start_date="20260101",
    end_date="20260131",
    event_names=["page_view", "purchase"]
)
```

---

## 外部リンク

- [megaton GitHub](https://github.com/mak00s/megaton)
- [Streamlit Documentation](https://docs.streamlit.io/)
