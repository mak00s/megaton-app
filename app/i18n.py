"""Internationalization (i18n) module for the Streamlit UI.

Dictionary-based translation with t("key") lookup.
No external dependencies.
"""
from __future__ import annotations

import streamlit as st

TRANSLATIONS: dict[str, dict[str, str]] = {
    "ja": {
        # --- Page ---
        "page.title": "AI分析アプリ",
        "page.heading": "📊 AI分析アプリ",

        # --- Sidebar: Settings ---
        "sidebar.settings": "設定",
        "sidebar.source": "データソース",
        "sidebar.start_date": "開始日",
        "sidebar.end_date": "終了日",
        "sidebar.limit": "取得件数",
        "sidebar.execute": "🚀 実行",
        "sidebar.execute_running": "⏳ 実行中...",

        # --- Sidebar: GA4 ---
        "ga4.property": "プロパティ",
        "ga4.dimensions": "ディメンション",
        "ga4.metrics": "メトリクス",
        "ga4.filter": "フィルタ条件",

        # --- Sidebar: GSC ---
        "gsc.site": "サイト",
        "gsc.dimensions": "ディメンション",
        "gsc.filter": "フィルタ条件",

        # --- Sidebar: Adobe Analytics ---
        "aa.company_id": "Company ID",
        "aa.org_id": "Org ID（任意）",
        "aa.rsid": "RSID",
        "aa.dimension": "ディメンション",
        "aa.metrics": "メトリクス",
        "aa.segment": "セグメントID",

        # --- Sidebar: BigQuery ---
        "bq.project_id": "プロジェクトID",
        "bq.sql": "SQL",
        "bq.sql_header": "SQL クエリ",
        "bq.datasets": "📁 データセット一覧",
        "bq.no_datasets": "データセットが見つかりません",
        "bq.dataset_error": "データセット取得エラー: {error}",

        # --- Filter columns ---
        "filter.field": "対象",
        "filter.operator": "演算子",
        "filter.value": "値",

        # --- Sidebar: Agent ---
        "agent.header": "🤖 AI Agent 連携",
        "agent.auto_watch": "JSON自動反映",
        "agent.auto_watch_help": "input/params.json の変更を2秒ごとに検知",
        "agent.auto_execute": "自動実行",
        "agent.auto_execute_help": "パラメータ読み込み後に自動でクエリ実行",
        "agent.params_updated": "📄 params.json: {time} 更新",
        "agent.params_none": "📄 params.json: なし",
        "agent.load_json": "📥 JSONを開く",
        "agent.json_header": "🤖 JSON (AI Agent用)",

        # --- Messages ---
        "msg.auth_error": "⚠️ 認証エラー: {error}",
        "msg.params_file_updated": "🔄 パラメータファイルが更新されました",
        "msg.params_validation_failed": "❌ params.json の検証に失敗しました",
        "msg.params_schema_mismatch": "params.json がスキーマ不一致です",
        "msg.params_applied": "📥 ファイルからパラメータを反映しました",
        "msg.params_loaded": "✓ パラメータを読み込みました",
        "msg.params_validation_error": "params.json の検証に失敗しました",
        "msg.params_not_found": "params.json が見つかりません",
        "msg.fetching": "データを取得中...",
        "msg.rows_fetched": "✓ {count} 行のデータを取得しました",
        "msg.no_data": "データが取得できませんでした",
        "msg.error": "エラー: {error}",
        "msg.enter_project_id": "プロジェクトIDを入力してください",
        "msg.enter_sql": "SQLを入力してください",
        "msg.enter_aa_company_id": "AA Company IDを入力してください",
        "msg.enter_aa_rsid": "AA RSIDを入力してください",
        "msg.enter_aa_dimension": "AAディメンションを入力してください",
        "msg.enter_aa_metrics": "AAメトリクスを1つ以上選択してください",
        "msg.pipeline_error": "パイプラインエラー: {error}",
        "msg.rows_summary": "📊 {total} 行",
        "msg.rows_filtered": "📊 {total} 行 → {filtered} 行",

        # --- Pipeline ---
        "pipeline.header": "🔧 結果の絞り込み・集計",
        "pipeline.transform": "**変換**",
        "pipeline.tf_date": "日付を YYYY-MM-DD に変換",
        "pipeline.tf_url_decode": "URLデコード",
        "pipeline.tf_strip_qs": "クエリ文字列を除去",
        "pipeline.tf_keep_params": "残すパラメータ（カンマ区切り、空=全除去）",
        "pipeline.tf_path_only": "パスのみ（ドメイン除去）",
        "pipeline.filter": "**フィルタ**",
        "pipeline.where": "条件式（pandas query構文）",
        "pipeline.columns_header": "**表示列**",
        "pipeline.columns": "列を選択（空=全列）",
        "pipeline.group_header": "**グループ集計**",
        "pipeline.group_cols": "グループ列",
        "pipeline.agg_caption": "集計関数を設定",
        "pipeline.head_header": "**表示行数**",
        "pipeline.head": "先頭N行（0=全行）",
        "pipeline.derived_cols": "集計後の列: {cols}",

        # --- Aggregation ---
        "agg.none": "（なし）",

        # --- Tabs ---
        "tab.table": "📋 テーブル",
        "tab.chart": "📈 チャート",
        "tab.save": "💾 保存",

        # --- Table ---
        "table.stats": "統計情報",
        "table.format_header": "表示フォーマット",
        "table.date_format": "日付表示形式",
        "table.date_fmt_ymd": "2026-03-12",
        "table.date_fmt_mdy": "Mar 12, 2026",
        "table.thousands_sep": "整数に3桁区切りを使う",
        "table.decimals": "小数桁数",
        "table.column_types": "列型ヒント（JSON）",
        "table.column_types_help": "{\"date\":\"date\",\"revenue\":\"currency\",\"orders\":\"int\"}",
        "msg.column_types_error": "列型ヒントエラー: {error}",

        # --- Chart ---
        "chart.x_axis": "X軸",
        "chart.y_axis": "Y軸",
        "chart.series": "系列（任意）",
        "chart.series_none": "（なし）",
        "chart.series_trimmed": "系列が多すぎるため上位 {shown} 件のみ表示（全 {total} 件）",
        "chart.type": "チャートタイプ",
        "chart.line": "折れ線",
        "chart.bar": "棒グラフ",

        # --- Save: Local ---
        "save.local_header": "ローカル保存",
        "save.filename": "ファイル名",
        "save.csv_download": "📥 CSV ダウンロード",
        "save.save_to_output": "💾 output/ に保存",
        "save.saved": "保存しました: {path}",
        "save.format_raw": "生値（推奨）",
        "save.format_formatted": "表示フォーマット値",
        "save.local_format": "ローカルCSVの保存形式",

        # --- Save: Google Sheets ---
        "save.sheets_header": "Google Sheets に保存",
        "save.sheet_url": "スプレッドシートURL",
        "save.sheet_name": "シート名",
        "save.mode": "保存モード",
        "save.mode_overwrite": "上書き",
        "save.mode_append": "追記",
        "save.mode_upsert": "アップサート",
        "save.upsert_keys": "キー列",
        "save.sheets_button": "📤 Google Sheets に保存",
        "save.enter_sheet_url": "スプレッドシートURLを入力してください",
        "save.select_keys": "キー列を選択してください",
        "save.sheets_saved": "✓ シート「{name}」に保存しました",
        "save.sheets_format": "Sheets書き込み形式",

        # --- Save: BigQuery ---
        "save.bq_header": "BigQuery に保存",
        "save.bq_project": "GCPプロジェクトID",
        "save.bq_dataset": "データセット",
        "save.bq_table": "テーブル",
        "save.bq_mode": "保存モード",
        "save.bq_button": "📤 BigQuery に保存",
        "save.bq_enter_all": "プロジェクトID、データセット、テーブルを入力してください",
        "save.bq_saved": "✓ {dest} に保存しました",
        "save.bq_raw_only": "BigQuery には常に生値を書き込みます（表示フォーマットは非適用）",
    },
    "en": {
        # --- Page ---
        "page.title": "AI Analytics",
        "page.heading": "📊 AI Analytics",

        # --- Sidebar: Settings ---
        "sidebar.settings": "Settings",
        "sidebar.source": "Data Source",
        "sidebar.start_date": "Start Date",
        "sidebar.end_date": "End Date",
        "sidebar.limit": "Row Limit",
        "sidebar.execute": "🚀 Run",
        "sidebar.execute_running": "⏳ Running...",

        # --- Sidebar: GA4 ---
        "ga4.property": "Property",
        "ga4.dimensions": "Dimensions",
        "ga4.metrics": "Metrics",
        "ga4.filter": "Filters",

        # --- Sidebar: GSC ---
        "gsc.site": "Site",
        "gsc.dimensions": "Dimensions",
        "gsc.filter": "Filters",

        # --- Sidebar: Adobe Analytics ---
        "aa.company_id": "Company ID",
        "aa.org_id": "Org ID (optional)",
        "aa.rsid": "RSID",
        "aa.dimension": "Dimension",
        "aa.metrics": "Metrics",
        "aa.segment": "Segment IDs",

        # --- Sidebar: BigQuery ---
        "bq.project_id": "Project ID",
        "bq.sql": "SQL",
        "bq.sql_header": "SQL Query",
        "bq.datasets": "📁 Datasets",
        "bq.no_datasets": "No datasets found",
        "bq.dataset_error": "Dataset fetch error: {error}",

        # --- Filter columns ---
        "filter.field": "Field",
        "filter.operator": "Operator",
        "filter.value": "Value",

        # --- Sidebar: Agent ---
        "agent.header": "🤖 AI Agent",
        "agent.auto_watch": "Auto-sync JSON",
        "agent.auto_watch_help": "Detect changes in input/params.json every 2s",
        "agent.auto_execute": "Auto-execute",
        "agent.auto_execute_help": "Run query automatically after loading params",
        "agent.params_updated": "📄 params.json: updated {time}",
        "agent.params_none": "📄 params.json: none",
        "agent.load_json": "📥 Load JSON",
        "agent.json_header": "🤖 JSON (for AI Agent)",

        # --- Messages ---
        "msg.auth_error": "⚠️ Auth error: {error}",
        "msg.params_file_updated": "🔄 Parameter file updated",
        "msg.params_validation_failed": "❌ params.json validation failed",
        "msg.params_schema_mismatch": "params.json schema mismatch",
        "msg.params_applied": "📥 Parameters loaded from file",
        "msg.params_loaded": "✓ Parameters loaded",
        "msg.params_validation_error": "params.json validation failed",
        "msg.params_not_found": "params.json not found",
        "msg.fetching": "Fetching data...",
        "msg.rows_fetched": "✓ Fetched {count} rows",
        "msg.no_data": "No data returned",
        "msg.error": "Error: {error}",
        "msg.enter_project_id": "Enter a Project ID",
        "msg.enter_sql": "Enter SQL",
        "msg.enter_aa_company_id": "Enter Adobe Analytics company ID",
        "msg.enter_aa_rsid": "Enter Adobe Analytics RSID",
        "msg.enter_aa_dimension": "Enter Adobe Analytics dimension",
        "msg.enter_aa_metrics": "Select at least one Adobe Analytics metric",
        "msg.pipeline_error": "Pipeline error: {error}",
        "msg.rows_summary": "📊 {total} rows",
        "msg.rows_filtered": "📊 {total} → {filtered} rows",

        # --- Pipeline ---
        "pipeline.header": "🔧 Filter & Aggregate",
        "pipeline.transform": "**Transform**",
        "pipeline.tf_date": "Format date as YYYY-MM-DD",
        "pipeline.tf_url_decode": "URL decode",
        "pipeline.tf_strip_qs": "Strip query string",
        "pipeline.tf_keep_params": "Keep params (comma-separated, empty=remove all)",
        "pipeline.tf_path_only": "Path only (strip domain)",
        "pipeline.filter": "**Filter**",
        "pipeline.where": "Expression (pandas query syntax)",
        "pipeline.columns_header": "**Columns**",
        "pipeline.columns": "Select columns (empty=all)",
        "pipeline.group_header": "**Group & Aggregate**",
        "pipeline.group_cols": "Group by",
        "pipeline.agg_caption": "Set aggregation functions",
        "pipeline.head_header": "**Row Limit**",
        "pipeline.head": "First N rows (0=all)",
        "pipeline.derived_cols": "Columns after aggregation: {cols}",

        # --- Aggregation ---
        "agg.none": "(none)",

        # --- Tabs ---
        "tab.table": "📋 Table",
        "tab.chart": "📈 Chart",
        "tab.save": "💾 Save",

        # --- Table ---
        "table.stats": "Statistics",
        "table.format_header": "Display Format",
        "table.date_format": "Date Format",
        "table.date_fmt_ymd": "2026-03-12",
        "table.date_fmt_mdy": "Mar 12, 2026",
        "table.thousands_sep": "Use thousands separators for integers",
        "table.decimals": "Decimal Places",
        "table.column_types": "Column Type Hints (JSON)",
        "table.column_types_help": "{\"date\":\"date\",\"revenue\":\"currency\",\"orders\":\"int\"}",
        "msg.column_types_error": "Column type hint error: {error}",

        # --- Chart ---
        "chart.x_axis": "X Axis",
        "chart.y_axis": "Y Axis",
        "chart.series": "Series (optional)",
        "chart.series_none": "(none)",
        "chart.series_trimmed": "Too many series; showing top {shown} of {total}",
        "chart.type": "Chart Type",
        "chart.line": "Line",
        "chart.bar": "Bar",

        # --- Save: Local ---
        "save.local_header": "Local Save",
        "save.filename": "Filename",
        "save.csv_download": "📥 Download CSV",
        "save.save_to_output": "💾 Save to output/",
        "save.saved": "Saved: {path}",
        "save.format_raw": "Raw values (Recommended)",
        "save.format_formatted": "Formatted display values",
        "save.local_format": "Local CSV output format",

        # --- Save: Google Sheets ---
        "save.sheets_header": "Save to Google Sheets",
        "save.sheet_url": "Spreadsheet URL",
        "save.sheet_name": "Sheet Name",
        "save.mode": "Save Mode",
        "save.mode_overwrite": "Overwrite",
        "save.mode_append": "Append",
        "save.mode_upsert": "Upsert",
        "save.upsert_keys": "Key Columns",
        "save.sheets_button": "📤 Save to Google Sheets",
        "save.enter_sheet_url": "Enter a spreadsheet URL",
        "save.select_keys": "Select key columns",
        "save.sheets_saved": "✓ Saved to sheet \"{name}\"",
        "save.sheets_format": "Sheets write format",

        # --- Save: BigQuery ---
        "save.bq_header": "Save to BigQuery",
        "save.bq_project": "GCP Project ID",
        "save.bq_dataset": "Dataset",
        "save.bq_table": "Table",
        "save.bq_mode": "Save Mode",
        "save.bq_button": "📤 Save to BigQuery",
        "save.bq_enter_all": "Enter Project ID, dataset, and table",
        "save.bq_saved": "✓ Saved to {dest}",
        "save.bq_raw_only": "BigQuery always writes raw values (display formatting is not applied)",
    },
}


def _get_lang() -> str:
    """Return current language code from session state."""
    return st.session_state.get("lang", "ja")


def t(key: str, **kwargs) -> str:
    """Look up a translated string by key.

    >>> t("msg.rows_fetched", count="1,234")
    '✓ Fetched 1,234 rows'  # when lang=en
    """
    lang = _get_lang()
    table = TRANSLATIONS.get(lang, TRANSLATIONS["ja"])
    text = table.get(key) or TRANSLATIONS["ja"].get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    return text


def t_option_map(keys_to_values: dict[str, str]) -> dict[str, str]:
    """Build {translated_label: internal_value} for selectbox/radio.

    Usage::

        opts = t_option_map({
            "save.mode_overwrite": "overwrite",
            "save.mode_append": "append",
        })
        label = st.selectbox("Mode", list(opts.keys()))
        value = opts[label]
    """
    return {t(k): v for k, v in keys_to_values.items()}


def t_reverse_map(keys_to_values: dict[str, str]) -> dict[str, str]:
    """Build {internal_value: translated_label} — inverse of t_option_map."""
    return {v: t(k) for k, v in keys_to_values.items()}


def translated_select_model(
    keys_to_values: dict[str, str],
    current_value: str | None = None,
) -> tuple[list[str], int, dict[str, str]]:
    """Build translated selectbox state from internal value.

    Returns (labels, default_index, {label: internal_value}).
    """
    options = t_option_map(keys_to_values)
    labels = list(options.keys())
    if not labels:
        return [], 0, options
    reverse = t_reverse_map(keys_to_values)
    default_label = reverse.get(current_value or "", labels[0])
    default_index = labels.index(default_label) if default_label in options else 0
    return labels, default_index, options


def t_options(keys: list[str]) -> list[str]:
    """Translate a list of keys. Useful for radio/tabs.

    Usage::

        tabs = st.tabs(t_options(["tab.table", "tab.chart", "tab.save"]))
    """
    return [t(k) for k in keys]


def init_language():
    """Initialise language in session state (call before set_page_config)."""
    if "lang" not in st.session_state:
        st.session_state["lang"] = "ja"


def language_selector():
    """Render a language toggle at the top of the sidebar."""
    lang = _get_lang()
    labels = {"ja": "日本語", "en": "English"}
    options = list(labels.keys())
    idx = options.index(lang) if lang in options else 0

    chosen = st.radio(
        "🌐",
        options,
        index=idx,
        format_func=lambda x: labels[x],
        horizontal=True,
        key="w_lang_selector",
        label_visibility="collapsed",
    )
    if chosen != lang:
        st.session_state["lang"] = chosen
        # Clear selectbox widget keys that store translated labels
        # to avoid stale-value errors after language switch.
        for k in ("w_save_mode_select", "w_save_bq_mode_select"):
            st.session_state.pop(k, None)
        st.rerun()
