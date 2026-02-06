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

### GSC検索クエリ取得

```bash
python scripts/query_gsc.py --days 14 --limit 1000
python scripts/query_gsc.py --site "https://example.com/" --start 2026-01-01 --end 2026-01-31
python scripts/query_gsc.py --filter "query:contains:渋谷"  # フィルタ付き
python scripts/query_gsc.py --json  # JSON出力（プログラム処理用）
```

### GA4データ取得

```bash
python scripts/query_ga4.py --days 7 --dimensions date --metrics sessions
python scripts/query_ga4.py --filter "sessionDefaultChannelGroup==Organic Search"
python scripts/query_ga4.py --property 254470346 --json
```

### BigQueryクエリ実行

```bash
python scripts/query_bq.py --project my-project --sql "SELECT * FROM dataset.table LIMIT 100"
python scripts/query_bq.py --project my-project --file query.sql
python scripts/query_bq.py --project my-project --list-datasets
```

### オプション一覧

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--days` | 直近N日間 | 14 |
| `--start`, `--end` | 日付範囲指定 | - |
| `--limit` | 結果件数 | 1000 (GSC), 10000 (GA4) |
| `--dimensions` | ディメンション | query (GSC), date (GA4) |
| `--filter` | フィルタ | - |
| `--json` | JSON出力 | テーブル出力 |
| `--output` | CSV出力ファイル | - |

### フィルタ書式

**GA4:** `field==value` 形式（複数はセミコロン区切り）
```bash
--filter "sessionDefaultChannelGroup==Organic Search;country==Japan"
```

**GSC:** `dimension:operator:expression` 形式（複数はセミコロン区切り）
```bash
--filter "query:contains:渋谷;page:includingRegex:/blog/"
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
3. 変更を検知して自動でUIに反映
4. 「自動実行」ONなら、そのままクエリ実行

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
