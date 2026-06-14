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

### 既存レポートの手書きpandasをチェーンへ移すとき（v1.4.2+）

`groupby + sum(min_count=1) + fillna_int + [key_cols]` の定型は、そのまま等価置換できます:

**順序が肝**: 元コードの `fillna_int` と `groupby` の**前後関係をそのまま再現**する（順序を変えると出力が変わる）。

ケースA — coerce が groupby の**前**（shibuya `_ch-m` 型。実データで全セル一致を確認済み v1.4.3）:
```python
# Before
fillna_int(df, ["users","cv","ad_cost"])
df = df.groupby(keys, dropna=False)[["users","cv","ad_cost"]].sum(min_count=1).reset_index()
fillna_int(df, ["users","cv","ad_cost"])
df = df[key_cols].sort_values(keys).reset_index(drop=True)

# After（to_int を先に。object型 ad_cost も coerce され group() が落とさない）
df = wrap(df).to_int(["users","cv","ad_cost"]).group(keys, dropna=False).sort(keys).select(key_cols).df
```

ケースB — coerce が groupby の**後**（全NaN群を保持したい型）:
```python
# Before
df = df.groupby(keys, dropna=False)[m].sum(min_count=1).reset_index()
fillna_int(df, m)
# After
df = wrap(df).group(keys, dropna=False, min_count=1).to_int(m).select(key_cols).df
```

落とし穴（実データ検証で判明）:
- **`group()` の自動metric検出は object dtype 列をスキップ**する。GA4の `advertiserAdCost` 等は object で来るので、**先に `.to_int()` で数値化**するか group に `metrics=` を明示する。
- `dropna=False` / `min_count=` を**元コードに合わせて指定**（省略すると NaNキー行や全NaN群の値が変わる）。
- カテゴリ分類は `apply_pattern_map`/`classify_by_pattern_map` → `.categorize(col, map, into=, default=)` に置換可（実GA4ページ746種×18パターンで全セル一致を確認済み 2026-06-12）。両者とも大小文字を区別。差が出るのは **非文字列/None 入力のみ**（`classify_by_pattern_map` は `str(v or "")` で空文字化、`.categorize()` は NaN→default）—GA4のページ列のような文字列列では一致。`classify_channel`/`reclassify_source_channel` 等の**行レベルのビジネス分類は残す**（ドメイン既定値を持つため）。
- `.month_key(col, into=, fmt=)` は手書き `pd.to_datetime(...).dt.strftime(...)` と等価（実データ確認済み）。
- **置換後は必ず本番コピーへ実走して全タブ全セル一致を確認**（手順は §6）。テストだけでは不十分。

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

## 6. レポート改修時の検証手順（出力が変わりうる変更）

チェーン化など計算経路を変える改修は、本番シートのコピーで全タブ一致を確認してから入れる:

1. 本番シートを共有ドライブ（例: WITH Report）へコピー
2. 改修**前**のコードで `python <app>/scripts/run_notebook.py <report>.py -p SHEET_URL=<コピー>` → 基準を取る
   （※ GA4は時間帯で値が動くため、基準取得と検証実行は近い時刻に。理想は改修前後で同一コピーへ連続実行）
3. 改修を適用 → 同じコピーへ再実行
4. 全タブ全セル比較（`fetch_worksheet_values` で読み比較するスクリプトを使う）。**一致**を確認
5. 一致でコミット、コピー削除

slqm 移行（2026-06）はこの手順で `_page`/`_page-d`/`_page-m`/`_all-m` 全セル一致を確認済み。
