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
| `date_range.start` | string | GA4/GSC | 開始日（YYYY-MM-DD） |
| `date_range.end` | string | GA4/GSC | 終了日（YYYY-MM-DD） |
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

---

## 認証情報

### サービスアカウント JSON

- 格納場所: `credentials/` ディレクトリ
- Git管理: **除外**（.gitignoreで設定済み）
- 推奨指定: 環境変数 `MEGATON_CREDS_PATH`（JSONファイル or JSONを1つ含むディレクトリ）

### Notebook での指定

```python
CREDS_PATH = "../credentials"  # ディレクトリを指定 → JSON選択UIが表示
```

### スクリプトでの指定

```python
import os
CREDS_PATH = os.environ["MEGATON_CREDS_PATH"]  # ファイル or ディレクトリを指定
```

---

## megaton API

### 初期化

```python
from megaton import start
from lib.credentials import resolve_service_account_path
mg = start.Megaton(resolve_service_account_path(), headless=True)
```

### GA4

```python
# アカウント・プロパティ一覧
mg.ga["4"].accounts  # [{"id": "...", "name": "...", "properties": [...]}]

# 選択
mg.ga["4"].account.select("ACCOUNT_ID")
mg.ga["4"].property.select("PROPERTY_ID")

# レポート
mg.report.set.dates("2026-01-01", "2026-01-31")
mg.report.run(d=["date"], m=["sessions"], filter_d="...", show=False)
df = mg.report.data
```

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
# サイト一覧
sites = mg.search.get.sites()

# 選択
mg.search.use("https://example.com/")

# レポート
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
