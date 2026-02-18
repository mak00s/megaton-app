# 使い方ガイド

## 1. Jupyter Notebook

対話的な探索分析、可視化、レポートに最適。
開発したノートブックは CLI / GitHub Actions でそのまま定期実行できる。

### セル構成

Jupyter Notebook は、パラメータセルと本処理セルの2段階構成が推奨される：

#### セル1: パラメータとセットアップ（`tags=["parameters"]`）

```python
# %% tags=["parameters"]
# ========== パラメータ（CLI実行時に上書き可能） ==========
PROPERTY_ID = "254800682"
START_DATE = "2025-06-01"
END_DATE = "2026-01-31"
OUTPUT_DIR = "output"

# ========== セットアップ ==========
import sys; sys.path.insert(0, "..")  # noqa: E702  ← サブディレクトリ（reports/等）の場合のみ
from setup import init; init()  # noqa: E702

# ========== ライブラリ読み込み ==========
import pandas as pd
import matplotlib.pyplot as plt
from lib.megaton_client import get_ga4, get_gsc
from lib.analysis import show
```

**ポイント:**
- `tags=["parameters"]` により、`run_notebook.py -p START_DATE=today-7d` でパラメータのみ上書き可能
- `init()` はパス解決・環境変数・モジュールリロードを一括実行
- `notebooks/` 直下のノートブックは `sys.path.insert` 不要
- サブディレクトリ（`reports/` 等）のノートブックは `sys.path.insert(0, "..")` を追加

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

# %% データ加工
df_organic = df[df["sessionDefaultChannelGroup"] == "Organic Search"]
df_pivot = df_organic.pivot_table(
    index="date",
    values=["sessions", "totalUsers"],
    aggfunc="sum"
)

# %% 可視化
df_pivot.plot(kind="line", figsize=(12, 6))
plt.title("Organic Search Traffic Trend")
plt.savefig(f"{OUTPUT_DIR}/trend.png")
show(df_pivot, save=f"{OUTPUT_DIR}/trend.csv")
```

**ポイント:**
- セル分割により、部分実行・デバッグが容易
- `show()` でコンテキストを節約（先頭20行のみ表示、必要ならCSV保存）
- CLI実行時は `MPLBACKEND=Agg` により `plt.show()` は表示されず、`plt.savefig()` のみ動作

### セルの流れ（ビジュアル）

```
┌─────────────────────────────────────────────────────┐
│ セル1: パラメータとセットアップ                        │
│ tags=["parameters"]                                 │
├─────────────────────────────────────────────────────┤
│ ✓ PROPERTY_ID, START_DATE, END_DATE を定義          │
│ ✓ init() でパス解決・環境変数設定                     │
│ ✓ 必要なライブラリをimport                           │
└──────────────────┬──────────────────────────────────┘
                   │
                   │ CLI実行時: -p START_DATE=today-7d で上書き
                   │ Jupyter実行時: そのまま使用
                   ▼
┌─────────────────────────────────────────────────────┐
│ セル2: データ取得                                     │
├─────────────────────────────────────────────────────┤
│ mg = get_ga4(PROPERTY_ID)                           │
│ mg.report.set.dates(START_DATE, END_DATE)           │
│ result = mg.report.run(d=[...], m=[...])            │
│ df = result.df                                      │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────┐
│ セル3: データ加工                                     │
├─────────────────────────────────────────────────────┤
│ df_filtered = df[df["channel"] == "Organic"]        │
│ df_pivot = df_filtered.pivot_table(...)             │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────┐
│ セル4: 可視化・出力                                   │
├─────────────────────────────────────────────────────┤
│ df_pivot.plot(...)                                  │
│ plt.savefig("output/chart.png")                     │
│ show(df_pivot, save="output/result.csv")            │
└─────────────────────────────────────────────────────┘
```

パラメータとセットアップは 1 セルに統合する。`tags=["parameters"]` を付けておけば
`run_notebook.py -p KEY=VALUE` でパラメータだけ上書きできる（import 文はマッチしない）。

### ノートブックの初期化

`notebooks/setup.py` の `init()` がパス解決・環境変数・モジュールリロードを一括で行う。

```python
# %% tags=["parameters"]
PROPERTY_ID = "254800682"
START_DATE = "2025-06-01"
END_DATE = "2026-01-31"

import sys; sys.path.insert(0, "..")  # noqa: E702  ← reports/ 等サブディレクトリの場合のみ
from setup import init; init()  # noqa: E702

from lib.megaton_client import get_ga4, get_gsc
from lib.analysis import show
```

`notebooks/` 直下のノートブックは `sys.path.insert` 不要（`setup.py` が同じ階層）。
サブディレクトリ（`reports/` 等）のノートブックは `sys.path.insert(0, "..")` で `notebooks/` を追加する。

### megaton ネイティブ API（推奨）

`get_ga4()` / `get_gsc()` はクレデンシャル自動選択・アカウント選択済みの megaton インスタンスを返す。
megaton の ReportResult / SearchResult のメソッドチェーンで後処理が可能。

```python
# GA4: ReportResult チェーン
mg = get_ga4(PROPERTY_ID)
mg.report.set.dates(START_DATE, END_DATE)
result = mg.report.run(d=["date", "landingPage"], m=["sessions"], show=False)
result.clean_url("landingPage").group("date").sort("date")
df = result.df

# GSC: SearchResult チェーン
mg = get_gsc(SITE_URL)
mg.search.set.dates(START_DATE, END_DATE)
result = mg.search.run(dimensions=["query", "page"], limit=25000)
result.decode().clean_url().normalize_queries().filter_impressions(min=10)
df = result.df
```

**ReportResult の主なメソッド:** `clean_url`, `group`, `sort`, `fill`, `to_int`, `replace`, `normalize`, `classify`, `categorize`

**SearchResult の主なメソッド:** `decode`, `clean_url`, `remove_params`, `normalize_queries`, `filter_clicks`, `filter_impressions`, `filter_ctr`, `filter_position`, `aggregate`, `classify`, `categorize`, `apply_if`

### CLI から実行

Jupyter で開発したノートブックを、そのまま CLI で実行できる。

```bash
# デフォルトパラメータで実行
python scripts/run_notebook.py notebooks/reports/yokohama_cv.py

# パラメータ上書き（日付テンプレート対応）
python scripts/run_notebook.py notebooks/reports/yokohama_cv.py \
  -p START_DATE=today-30d -p END_DATE=today
```

`MPLBACKEND=Agg` で実行するため、`plt.show()` は呼ばれてもGUI表示されない。CSV保存などの出力はそのまま動作する。

### Jupytext（.ipynb ↔ .py 同期）

AI Agent が Notebook を直接編集すると壊れることがあるため、Jupytext で同期して運用。

**編集フロー:**
1. AI Agent は `.py` ファイルを編集
2. 編集後に同期:
   ```bash
   jupytext --sync notebooks/**/*.ipynb
   ```
3. Jupyter で `.ipynb` を開いて実行

**手動で .ipynb を編集した場合:**
```bash
jupytext --sync notebooks/**/*.ipynb
```

---

## 2. CLIスクリプト（AI Agent推奨）

AI Agent がデータを取得する際は、CLIスクリプトを使用。高速で確実。

### 統合CLI実行（推奨）

```bash
# source を見て自動分岐（ga4/gsc/bigquery）
python scripts/query.py --params input/params.json

# 同期実行 + 結果フィルタ（ジョブ不要）
python scripts/query.py --params input/params.json --json --where "clicks > 10" --sort "clicks DESC" --head 20

# 非同期ジョブとして投入
python scripts/query.py --submit --params input/params.json

# ジョブ状態確認
python scripts/query.py --status <job_id>

# ジョブキャンセル
python scripts/query.py --cancel <job_id>

# ジョブ結果確認
python scripts/query.py --result <job_id>

# ジョブ結果の先頭N行のみ取得
python scripts/query.py --result <job_id> --head 20

# ジョブ結果の要約統計のみ取得
python scripts/query.py --result <job_id> --summary

# ジョブ結果をフィルタ/集計して取得
python scripts/query.py --result <job_id> --json --where "clicks > 10" --sort "clicks DESC" --head 20
python scripts/query.py --result <job_id> --json --group-by "page" --aggregate "sum:clicks,mean:ctr" --sort "sum_clicks DESC"
python scripts/query.py --result <job_id> --json --columns "query,clicks,impressions"

# ジョブ一覧
python scripts/query.py --list-jobs

# JSON出力
python scripts/query.py --params input/params.json --json

# CSV保存
python scripts/query.py --params input/params.json --output output/result.csv

# 一覧取得
python scripts/query.py --list-ga4-properties
python scripts/query.py --list-gsc-sites
python scripts/query.py --list-bq-datasets --project my-project
```

### オプション一覧

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--params` | スキーマ検証済みJSON入力 | `input/params.json` |
| `--submit` | ジョブを非同期投入 | OFF |
| `--status <job_id>` | ジョブ状態の表示 | - |
| `--cancel <job_id>` | 実行中/待機中ジョブのキャンセル | - |
| `--result <job_id>` | ジョブ結果情報の表示 | - |
| `--head <N>` | `--result` で先頭N行を返す | - |
| `--summary` | `--result` で要約統計を返す | OFF |
| `--where` | 同期実行/`--result` で行フィルタ（pandas query） | - |
| `--sort` | 同期実行/`--result` でソート（`col DESC,col2 ASC`） | - |
| `--columns` | 同期実行/`--result` で列選択（カンマ区切り） | - |
| `--group-by` | 同期実行/`--result` でグループ列（カンマ区切り） | - |
| `--aggregate` | 同期実行/`--result` で集計（`sum:clicks` 形式） | - |
| `--list-jobs` | ジョブ一覧の表示 | OFF |
| `--job-limit` | ジョブ一覧の件数上限 | 20 |
| `--list-ga4-properties` | GA4プロパティ一覧 | OFF |
| `--list-gsc-sites` | GSCサイト一覧 | OFF |
| `--list-bq-datasets` | BigQueryデータセット一覧 | OFF |
| `--project` | データセット一覧取得対象プロジェクト | - |
| `--json` | JSON出力 | テーブル出力 |
| `--output` | CSV出力ファイル | - |

`--params` 実行時は `schema_version: "1.0"` を必須検証し、`source` とキー整合性が崩れている場合は実行前にエラー終了します。
`--head` と `--summary` は `--result` と併用する。
`--group-by` と `--aggregate` は同時指定が必須。
`--summary` は `--result` 専用で、パイプラインオプションとは同時指定不可。
`--json` 指定時は成功・失敗ともに構造化JSONを返す（成功: `status=ok`、失敗: `status=error`）。

### ジョブ管理の保存先

- ジョブレコード: `output/jobs/records/*.json`
- 実行ログ: `output/jobs/logs/*.log`
- 結果CSV: `output/jobs/artifacts/*.csv`

### フィルタ書式

`input/params.json` 内で指定する。

**GA4:** `filter_d` に `field==value` 形式（複数はセミコロン区切り）
```json
"filter_d": "sessionDefaultChannelGroup==Organic Search;country==Japan"
```

**GSC:** `filter` に `dimension:operator:expression` 形式（複数はセミコロン区切り）
```json
"filter": "query:contains:渋谷;page:includingRegex:/blog/"
```

GSC演算子: `contains`, `notContains`, `equals`, `notEquals`, `includingRegex`, `excludingRegex`

### カスタムスクリプト

独自スクリプトで megaton を使う場合は、headless モードで初期化。

```python
from megaton import start

# headless モードで初期化（UIなし）
mg = start.Megaton("credentials/sa-xxx.json", headless=True)

# GA4 アカウント・プロパティを直接指定
mg.ga['4'].account.select("ACCOUNT_ID")
mg.ga['4'].property.select("PROPERTY_ID")

# レポート実行（show=False で表示をスキップ）
mg.report.set.dates(start_date, end_date)
mg.report.run(d=[...], m=[...], show=False)
df = mg.report.data
```

**ポイント:**
- `headless=True`: UI（ipywidgets）を使わない
- `show=False`: レポート実行後の自動表示をスキップ
- アカウント・プロパティは ID を直接指定
- 認証JSONは `MEGATON_CREDS_PATH` で指定（未指定時は `credentials/*.json` を自動探索）
- BigQuery のパラメータ化クエリ（`query_bq(..., params=...)`）は
  `GOOGLE_APPLICATION_CREDENTIALS` を優先して使用。未設定時は
  `MEGATON_CREDS_PATH` / `credentials/*.json` から自動選択（`creds_hint` 優先）

---

## 3. Streamlit UI（対話型分析）

AI Agent と人間が対話しながらデータ分析を行うためのWeb UI。

### 起動方法

```bash
streamlit run app/streamlit_app.py
# → http://localhost:8501 でアクセス
```

### UI機能

- データソース選択（GA4 / GSC / BigQuery）
- プロパティ/サイト選択ドロップダウン（動的取得）
- 日付範囲入力
- BigQuery: SQL入力エリア、データセット一覧表示
- テーブル/チャート表示（折れ線/棒グラフ）
- CSV保存/ダウンロード

### AI Agent 連携

**自動反映機能:**
1. AI Agent が `input/params.json` にパラメータを書き込む
2. Streamlit UIが2秒ごとにファイルを監視（更新時刻 + 実質差分）
3. JSONスキーマを検証（不正なら反映しない）
4. 変更を検知して自動でUIに反映（空白/インデント/キー順のみの差分は反映しない）
5. 「自動実行」ONなら、そのままクエリ実行

**必須項目（完全移行）:**
- `schema_version: "1.0"`
- `source` に応じた必須項目（詳細は `docs/REFERENCE.md` と `schemas/query-params.schema.json`）

**UIの設定（サイドバー）:**
- 「JSON自動反映」: ON/OFFでファイル監視を切り替え
- 「自動実行」: ONにするとパラメータ反映後に自動でクエリ実行
- 「JSONを開く」: 手動でparams.jsonを読み込み

### フロー

1. 人間が自然言語で要求（例: 「直近7日間のOrganic Search推移」）
2. AI Agent が解釈して `input/params.json` を更新
3. Streamlit UIに自動反映
4. 人間がUIで日付などを確認・修正
5. 「実行」ボタン押下 → 結果表示
6. 「CSV保存」→ AI Agent が `output/result_*.csv` を読んで分析続行

---

## 設定管理（Google Sheets）

分析ごとに可変の設定（対象サイト一覧、フィルタ条件、閾値など）は Google Sheets から読み込む。

**設定シートの例:**
| site_name | ga4_property_id | gsc_site_url | min_impressions |
|-----------|-----------------|--------------|-----------------|
| サイトA   | 123456789       | https://...  | 100             |

**読み込み方法:**
```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
config_df = mg.sheet.df()
sites = config_df.to_dict('records')
```

---

## テスト

### 実行方法

```bash
# 全テスト
python -m pytest -q

# レイヤ別（unit / contract / integration）
python -m pytest -q -m unit
python -m pytest -q -m contract
python -m pytest -q -m integration

# query.py のカバレッジ（CIと同じ閾値）
python -m pytest -q --cov=scripts.query --cov-report=term-missing --cov-fail-under=90
```

### 現在の目安（2026-02-07時点）

- `python -m pytest -q`: `276 passed`
- `scripts/query.py` coverage: `98%`
