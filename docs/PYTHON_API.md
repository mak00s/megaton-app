# Python API ガイド（notebook / script から megaton_lib を使う）

CLI は [REFERENCE.md](REFERENCE.md)。このページは **Python から直接呼ぶとき**の最短ルート。

## 最初に覚える入口は1つ

```python
from megaton_lib.notebook import (
    get_ga4, get_gsc, get_bq_client,        # クライアント（認証は自動ルーティング）
    query_ga4, query_gsc, query_bq,         # 1呼び出しでDataFrame
    wrap,                                   # 任意のDataFrameをチェーンAPIへ
    resolve_date, resolve_month,            # "prev-month-start" → "2026-05-01"
    read_sheet_table, save_sheet_table, upsert_or_skip,  # Sheets I/O
    start_report_run,                       # レポートscaffold
    fetch_for_sites,                        # マルチサイトGSC取得
    fillna_int, show,
)
```

すべて遅延importなので、使わない機能のoptional依存は不要。

## 1. クエリ → 変換 → 保存（チェーンAPIが正準形）

```python
mg = get_ga4("254800682")
mg.report.set.dates(resolve_date("prev-month-start"), resolve_date("prev-month-end"))
result = mg.report.run(d=["date", "sessionSource"], m=["sessions"], show=False)

monthly = (
    result
    .month_key("date", into="month", fmt="%Y-%m")   # 月キー生成（フォーマット統一）
    .normalize("sessionSource", {"google": r"google"})
    .group("month")
    .to_int()
    .sort("month")
)
mg.save.to.sheet("monthly", monthly)   # Resultをそのまま渡せる（.df不要）
```

- 複合フィルタ: `filter_d={"and": ["date==2026-05-01", {"or": ["country==Japan", "country==Taiwan"]}]}`
- BigQuery / Sheets / CSV 由来のDataFrameも `wrap(df)` で同じ語彙が使える:

```python
df = query_bq("corp-project", "SELECT ...")
top = wrap(df).categorize("page", {"news": r"/information/"}).group("page_category").sort("pv", ascending=False)
```

チェーンメソッド一覧は megaton の [api-reference.md](../../megaton/docs/api-reference.md)（ReportResult メソッドチェーン節）。

## 2. 日付は `megaton_lib.dates` だけ覚える

```python
from megaton_lib.dates import (
    resolve_date,          # "prev-month-start" / "today-7d" / "prev-prev-month-end" → "YYYY-MM-DD"
    resolve_month,         # "prev-month" → "202605"
    previous_month_window, # → (date(2026,5,1), date(2026,5,31))
    month_ranges_for_year, parse_summary_tokens, now_in_tz,
    resolve_effective_months_ago,   # GHA月初切替
)
```

`date_template` / `date_utils` / `periods` / 旧notebooks `date_periods` を個別importしない。

## 3. レポートnotebookの定型は `start_report_run`

```python
run = start_report_run(
    "slqm",
    property_id=PROPERTY_ID,
    start_date=START_DATE,     # テンプレート可
    end_date=END_DATE,
)
mg, tracker = run.mg, run.tracker
# ...集計セル...
run.save_sheet(gs_url=SHEET_URL, sheet_name="_page", df=df)   # mgは自動で渡る
run.finish()                   # 収集済みerrorsがあればstatus=failed
```

- notebookでは begin/end ペア（`with` はセルを跨げない）。scriptでは `with start_report_run(...) as run:` で例外を自動記録
- Box/Gmail等の配送は `run.on_finish(callback)` で呼び出し側が注入

## 4. Sheets I/O の正準形

```python
df = read_sheet_table(mg, sheet_url=URL, sheet_name="config")
save_sheet_table(mg, sheet_url=URL, sheet_name="out", df=df)
upsert_or_skip(mg, "monthly", df, keys=["month"])
```

低レベル(batchUpdate等)は `gspread_lowlevel`（全呼び出しにretry内蔵、429は30秒フロア）。

## 5. してはいけないこと

- `mg.ga["4"]...` / `mg.search.get...` 等の **内部アクセス禁止** — `mg.properties()` / `mg.sites()` / `mg.use_property()` を使う（megaton 1.4+）
- 例外は `except Exception` でなく `megaton.errors`（`BadRequest`, `ApiDisabled`, `BadPermission`...）を優先的にcatch
- 日付・正規表現分類・月フォーマットの**手書き再実装**（このページの正準形を使う）
