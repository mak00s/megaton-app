# 使い方ガイド

## 1. Jupyter Notebook

対話的な探索分析、可視化、メモに最適。

### セル構成

```
セル1: 設定・初期化・GA4選択
  - CREDS_PATH, GCP_PROJECT_ID, CONFIG_SHEET_URL を直書き
  - megaton 初期化 → JSON選択UI → GA4選択UI が表示される

セル2: レポート期間設定
  - start_date, end_date を設定

セル3以降: 本処理
  - データ取得・加工・集計・可視化
```

### Jupytext（.ipynb ↔ .py 同期）

AI Agent が Notebook を直接編集すると壊れることがあるため、Jupytext で同期して運用。

**編集フロー:**
1. AI Agent は `.py` ファイルを編集
2. 編集後に同期:
   ```bash
   jupytext --sync notebooks/*.ipynb
   ```
3. Jupyter で `.ipynb` を開いて実行

**手動で .ipynb を編集した場合:**
```bash
jupytext --sync notebooks/*.ipynb
```

---

## 2. CLIスクリプト（AI Agent推奨）

AI Agent がデータを取得する際は、CLIスクリプトを使用。高速で確実。

### 統合CLI実行（推奨）

```bash
# source を見て自動分岐（ga4/gsc/bigquery）
python scripts/query.py --params input/params.json

# 非同期ジョブとして投入
python scripts/query.py --submit --params input/params.json

# ジョブ状態確認
python scripts/query.py --status <job_id>

# ジョブ結果確認
python scripts/query.py --result <job_id>

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
| `--result <job_id>` | ジョブ結果情報の表示 | - |
| `--list-jobs` | ジョブ一覧の表示 | OFF |
| `--job-limit` | ジョブ一覧の件数上限 | 20 |
| `--list-ga4-properties` | GA4プロパティ一覧 | OFF |
| `--list-gsc-sites` | GSCサイト一覧 | OFF |
| `--list-bq-datasets` | BigQueryデータセット一覧 | OFF |
| `--project` | データセット一覧取得対象プロジェクト | - |
| `--json` | JSON出力 | テーブル出力 |
| `--output` | CSV出力ファイル | - |

`--params` 実行時は `schema_version: "1.0"` を必須検証し、`source` とキー整合性が崩れている場合は実行前にエラー終了します。

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
2. Streamlit UIが2秒ごとにファイルを監視
3. JSONスキーマを検証（不正なら反映しない）
4. 変更を検知して自動でUIに反映
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
