# 使い方ガイド

## 1. Jupyter Notebook

対話的な探索分析、可視化、メモに最適。

### セル構成

```
セル1: 設定・初期化・GA4選択
  - CREDS_PATH, GCP_PROJECT_ID, CONFIG_SHEET_URL を直書き
  - megaton 初期化 → JSON選択UI → GA4選択UI が表示される

セル2: 認証チェック
  - Search Console, Google Sheets, BigQuery の状態を info 表示
  - ✓/✗/- で結果表示、エラーでも続行可能

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
python scripts/query_gsc.py --days 14 --limit 20
python scripts/query_gsc.py --site "https://example.com/" --start 2026-01-01 --end 2026-01-31
python scripts/query_gsc.py --json  # JSON出力（プログラム処理用）
```

### GA4データ取得

```bash
python scripts/query_ga4.py --days 7 --dimensions date --metrics sessions
python scripts/query_ga4.py --filter "defaultChannelGroup==Organic Search"
python scripts/query_ga4.py --property 254470346 --json
```

### オプション一覧

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--days` | 直近N日間 | 14 |
| `--start`, `--end` | 日付範囲指定 | - |
| `--limit` | 結果件数 | 20 (GSC), 100 (GA4) |
| `--dimensions` | ディメンション | query (GSC), date (GA4) |
| `--filter` | フィルタ（GA4のみ） | - |
| `--json` | JSON出力 | テーブル出力 |

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

## 3. Gradio UI（対話型分析）

AI Agent と人間が対話しながらデータ分析を行うためのWeb UI。

### 起動方法

```bash
python app/main.py
# → http://localhost:7860 でアクセス
```

### UI機能

- データソース選択（GA4 / GSC）
- プロパティ/サイト選択ドロップダウン（動的取得）
- 日付範囲入力
- JSONパラメータ編集
- JSON ⇔ UI 双方向同期
- テーブル/チャート表示
- CSV保存（`output/` フォルダに保存）

### フロー

1. 人間が自然言語で要求（例: 「渋谷サイトの直近7日間のOrganic Search推移」）
2. AI Agent が解釈してJSONパラメータを生成
3. 人間がJSONをGradio UIに貼り付け
4. 「↑ UIに読み込み」でUI要素に反映
5. UIで日付などを修正 →「↓ JSONに反映」
6. 「実行」ボタン押下 → 結果表示
7. 「CSV保存」→ AI Agent が `output/result_*.csv` を読んで分析続行

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
config_df = mg.sheet.to_dataframe()
sites = config_df.to_dict('records')
```
