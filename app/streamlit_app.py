"""Streamlit UI メインアプリ"""
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

st.set_page_config(
    page_title="AI分析アプリ",
    page_icon="📊",
    layout="wide",
)

# === パラメータファイル監視 ===

PARAMS_FILE = Path("input/params.json")

def load_params_from_file():
    """外部JSONファイルからパラメータを読み込む"""
    if not PARAMS_FILE.exists():
        return None, None, []
    try:
        mtime = PARAMS_FILE.stat().st_mtime
        with open(PARAMS_FILE, "r", encoding="utf-8") as f:
            raw_params = json.load(f)
        params, errors = validate_params(raw_params)
        return params, mtime, errors
    except json.JSONDecodeError as e:
        return None, None, [{
            "error_code": "INVALID_JSON",
            "message": f"Invalid JSON: {e}",
            "path": "$",
            "hint": "Fix JSON syntax in input/params.json."
        }]
    except IOError as e:
        return None, None, [{
            "error_code": "FILE_IO_ERROR",
            "message": f"Failed to read params.json: {e}",
            "path": "$",
            "hint": "Check file permissions and file path."
        }]

def apply_params_to_session(params):
    """読み込んだパラメータをセッションに反映（ウィジェットのkeyも更新）"""
    if params is None:
        return False

    st.session_state["loaded_params"] = params
    st.session_state["params_applied"] = True

    # ウィジェットのkeyを直接更新（これでUIに即座に反映される）
    source = params.get("source", "ga4").lower()

    # 日付
    date_range = params.get("date_range", {})
    if date_range.get("start"):
        st.session_state["w_start_date"] = datetime.strptime(date_range["start"], "%Y-%m-%d").date()
    if date_range.get("end"):
        st.session_state["w_end_date"] = datetime.strptime(date_range["end"], "%Y-%m-%d").date()

    # 取得件数
    if "limit" in params:
        st.session_state["w_limit"] = params["limit"]

    # ディメンション
    if "dimensions" in params:
        if source == "gsc":
            st.session_state["w_gsc_dimensions"] = params["dimensions"]
        else:
            st.session_state["w_ga4_dimensions"] = params["dimensions"]

    # GA4固有
    if source == "ga4":
        if "property_id" in params:
            st.session_state["w_ga4_property_id"] = params["property_id"]
        if "metrics" in params:
            st.session_state["w_ga4_metrics"] = params["metrics"]
        if "filter_d" in params:
            st.session_state["w_ga4_filter"] = params["filter_d"]

    # GSC固有
    if source == "gsc":
        if "site_url" in params:
            st.session_state["w_gsc_site"] = params["site_url"]
        if "filter" in params:
            st.session_state["w_gsc_filter"] = params["filter"]

    # BigQuery固有
    if source == "bigquery":
        if "project_id" in params:
            st.session_state["w_bq_project"] = params["project_id"]
        if "sql" in params:
            st.session_state["w_bq_sql"] = params["sql"]

    return True

def check_file_updated():
    """ファイル更新をチェック"""
    if not PARAMS_FILE.exists():
        return False

    current_mtime = PARAMS_FILE.stat().st_mtime
    last_mtime = st.session_state.get("last_params_mtime", 0)

    if current_mtime > last_mtime:
        st.session_state["last_params_mtime"] = current_mtime
        return True
    return False

# === 共通モジュールからインポート ===
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.megaton_client import (
    get_megaton,
    get_ga4_properties as _get_ga4_properties,
    query_ga4,
    get_gsc_sites as _get_gsc_sites,
    query_gsc,
    get_bq_datasets as _get_bq_datasets,
    query_bq,
    save_to_sheet,
)
from lib.params_validator import validate_params

# Streamlit用キャッシュラッパー
@st.cache_data(ttl=300)
def get_ga4_properties():
    return _get_ga4_properties()

@st.cache_data(ttl=300)
def get_gsc_sites():
    return _get_gsc_sites()

@st.cache_data(ttl=300)
def get_bq_datasets(project_id):
    return _get_bq_datasets(project_id)

@st.cache_data(ttl=60)
def execute_ga4_query(property_id, start_date, end_date, dimensions, metrics, filter_d, limit):
    return query_ga4(property_id, start_date, end_date, dimensions, metrics, filter_d, limit)

@st.cache_data(ttl=60)
def execute_gsc_query(site_url, start_date, end_date, dimensions, limit, dimension_filter=None):
    return query_gsc(site_url, start_date, end_date, dimensions, limit, dimension_filter)

def parse_gsc_filter(filter_str: str):
    """GSCフィルタ文字列をパース（API用）"""
    if not filter_str or not filter_str.strip():
        return None
    filters = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)
        if len(parts) == 3:
            filters.append({
                "dimension": parts[0],
                "operator": parts[1],
                "expression": parts[2]
            })
    return filters if filters else None


# === フィルタ用ヘルパー関数 ===

# GA4演算子
GA4_OPERATORS = ["==", "!=", "=@", "!@", "=~", "!~", ">", ">=", "<", "<="]
GA4_OPERATOR_LABELS = {
    "==": "等しい",
    "!=": "等しくない", 
    "=@": "含む",
    "!@": "含まない",
    "=~": "正規表現一致",
    "!~": "正規表現不一致",
    ">": "より大きい",
    ">=": "以上",
    "<": "より小さい",
    "<=": "以下",
}

# GSC演算子
GSC_OPERATORS = ["contains", "notContains", "equals", "notEquals", "includingRegex", "excludingRegex"]
GSC_OPERATOR_LABELS = {
    "contains": "含む",
    "notContains": "含まない",
    "equals": "等しい",
    "notEquals": "等しくない",
    "includingRegex": "正規表現一致",
    "excludingRegex": "正規表現不一致",
}


def parse_ga4_filter_to_df(filter_str: str) -> pd.DataFrame:
    """GA4フィルタ文字列をDataFrameにパース"""
    if not filter_str or not filter_str.strip():
        return pd.DataFrame(columns=["対象", "演算子", "値"])
    
    rows = []
    for part in filter_str.split(";"):
        part = part.strip()
        if not part:
            continue
        # 演算子でマッチ（長い順に試す）
        for op in sorted(GA4_OPERATORS, key=len, reverse=True):
            if op in part:
                idx = part.index(op)
                field = part[:idx]
                value = part[idx + len(op):]
                rows.append({"対象": field, "演算子": op, "値": value})
                break
    
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["対象", "演算子", "値"])


def serialize_ga4_filter_from_df(df: pd.DataFrame) -> str:
    """DataFrameからGA4フィルタ文字列を生成"""
    if df is None or df.empty:
        return ""
    parts = []
    for _, row in df.iterrows():
        if row["対象"] and row["演算子"] and row["値"]:
            parts.append(f"{row['対象']}{row['演算子']}{row['値']}")
    return ";".join(parts)


def parse_gsc_filter_to_df(filter_str: str) -> pd.DataFrame:
    """GSCフィルタ文字列をDataFrameにパース"""
    if not filter_str or not filter_str.strip():
        return pd.DataFrame(columns=["対象", "演算子", "値"])
    
    rows = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)
        if len(parts) == 3:
            rows.append({"対象": parts[0], "演算子": parts[1], "値": parts[2]})
    
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["対象", "演算子", "値"])


def serialize_gsc_filter_from_df(df: pd.DataFrame) -> str:
    """DataFrameからGSCフィルタ文字列を生成"""
    if df is None or df.empty:
        return ""
    parts = []
    for _, row in df.iterrows():
        if row["対象"] and row["演算子"] and row["値"]:
            parts.append(f"{row['対象']}:{row['演算子']}:{row['値']}")
    return ";".join(parts)


def execute_bq_query(project_id, sql):
    return query_bq(project_id, sql)


# === UI ===

st.title("📊 AI分析アプリ")

# === ファイル監視セクション ===

# セッション初期化
if "auto_watch" not in st.session_state:
    st.session_state["auto_watch"] = True
if "auto_execute" not in st.session_state:
    st.session_state["auto_execute"] = False
if "params_validation_errors" not in st.session_state:
    st.session_state["params_validation_errors"] = []

# 自動リフレッシュ（ファイル監視用：2秒ごと）
if st.session_state.get("auto_watch", True):
    st_autorefresh(interval=2000, limit=None, key="file_watcher_refresh")

# ファイル変更チェック用フラグ
file_just_updated = False

# ファイル変更チェック（メインスクリプト内で実行）
if st.session_state.get("auto_watch", True) and check_file_updated():
    params, _, errors = load_params_from_file()
    if params:
        apply_params_to_session(params)
        st.session_state["params_validation_errors"] = []
        st.toast("🔄 パラメータファイルが更新されました", icon="📄")
        file_just_updated = True
        if st.session_state.get("auto_execute", False):
            st.session_state["auto_execute_pending"] = True
    elif errors:
        st.session_state["params_validation_errors"] = errors
        st.toast("❌ params.json の検証に失敗しました", icon="⚠️")

with st.sidebar:
    with st.expander("🤖 AI Agent 連携", expanded=True):
        st.session_state["auto_watch"] = st.toggle(
            "JSON自動反映",
            value=st.session_state.get("auto_watch", True),
            help="input/params.json の変更を2秒ごとに検知"
        )
        st.session_state["auto_execute"] = st.toggle(
            "自動実行",
            value=st.session_state.get("auto_execute", False),
            help="パラメータ読み込み後に自動でクエリ実行"
        )

        # ファイル状態表示
        if PARAMS_FILE.exists():
            mtime = datetime.fromtimestamp(PARAMS_FILE.stat().st_mtime)
            st.caption(f"📄 params.json: {mtime.strftime('%H:%M:%S')} 更新")
        else:
            st.caption("📄 params.json: なし")

        # 手動読み込みボタン
        if st.button("📥 JSONを開く", use_container_width=True):
            params, mtime, errors = load_params_from_file()
            if params:
                apply_params_to_session(params)
                st.session_state["last_params_mtime"] = mtime
                st.session_state["params_validation_errors"] = []
                st.success("✓ パラメータを読み込みました")
                st.rerun()
            elif errors:
                st.session_state["params_validation_errors"] = errors
                st.error("params.json の検証に失敗しました")
            else:
                st.warning("params.json が見つかりません")

# サイドバー
with st.sidebar:
    st.header("設定")

    validation_errors = st.session_state.get("params_validation_errors", [])
    if validation_errors:
        st.error("params.json がスキーマ不一致です")
        for err in validation_errors:
            st.caption(f"`{err['error_code']}` {err['path']} - {err['message']}")

    # 読み込み済みパラメータを取得
    lp = st.session_state.get("loaded_params", {})

    # パラメータ反映時の通知
    if st.session_state.get("params_applied"):
        st.info("📥 ファイルからパラメータを反映しました")
        st.session_state["params_applied"] = False

    # データソース選択
    source_map = {"ga4": "GA4", "gsc": "GSC", "bigquery": "BigQuery"}
    default_source = source_map.get(lp.get("source", "ga4").lower(), "GA4")
    source = st.radio("データソース", ["GA4", "GSC", "BigQuery"], horizontal=True,
                      index=["GA4", "GSC", "BigQuery"].index(default_source))

    st.divider()

    # BigQuery以外は日付範囲を表示
    if source != "BigQuery":
        # 日付範囲（セッション状態があればそれを使用、なければデフォルト）
        if "w_start_date" not in st.session_state:
            date_range = lp.get("date_range", {})
            st.session_state["w_start_date"] = datetime.strptime(date_range["start"], "%Y-%m-%d").date() if date_range.get("start") else (datetime.now() - timedelta(days=14)).date()
            st.session_state["w_end_date"] = datetime.strptime(date_range["end"], "%Y-%m-%d").date() if date_range.get("end") else (datetime.now() - timedelta(days=1)).date()

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("開始日", key="w_start_date")
        with col2:
            end_date = st.date_input("終了日", key="w_end_date")

        st.divider()

    if source == "GA4":
        # GA4設定
        properties = get_ga4_properties()
        property_options = {p["display"]: p["id"] for p in properties}

        # プロパティIDからdisplay名を逆引き（セッションステートまたはloaded_paramsから）
        default_prop_idx = 0
        loaded_prop_id = st.session_state.get("w_ga4_property_id") or lp.get("property_id", "")
        for i, (display, pid) in enumerate(property_options.items()):
            if pid == loaded_prop_id:
                default_prop_idx = i
                break

        selected_property = st.selectbox("プロパティ", list(property_options.keys()), index=default_prop_idx)
        property_id = property_options[selected_property]

        # ディメンション（初期化）
        all_dimensions = ["date", "sessionDefaultChannelGroup", "sessionSource", "sessionMedium",
                         "pagePath", "landingPage", "deviceCategory", "country"]
        if "w_ga4_dimensions" not in st.session_state:
            st.session_state["w_ga4_dimensions"] = lp.get("dimensions", ["date"]) if lp.get("source", "").lower() == "ga4" else ["date"]
        dimensions = st.multiselect("ディメンション", all_dimensions, key="w_ga4_dimensions")

        # メトリクス（初期化）
        all_metrics = ["sessions", "activeUsers", "newUsers", "screenPageViews",
                      "bounceRate", "averageSessionDuration", "conversions"]
        if "w_ga4_metrics" not in st.session_state:
            st.session_state["w_ga4_metrics"] = lp.get("metrics", ["sessions", "activeUsers"]) if lp.get("source", "").lower() == "ga4" else ["sessions", "activeUsers"]
        metrics = st.multiselect("メトリクス", all_metrics, key="w_ga4_metrics")

        # フィルタ（初期化）
        if "w_ga4_filter" not in st.session_state:
            st.session_state["w_ga4_filter"] = lp.get("filter_d", "") if lp.get("source", "").lower() == "ga4" else ""
        
        # フィルタをDataFrameにパース
        filter_df = parse_ga4_filter_to_df(st.session_state.get("w_ga4_filter", ""))
        
        with st.expander("フィルタ条件", expanded=bool(len(filter_df))):
            # よく使うディメンション
            ga4_filter_dims = ["sessionDefaultChannelGroup", "sessionSource", "sessionMedium", 
                               "pagePath", "landingPage", "deviceCategory", "country", "city"]
            
            edited_filter_df = st.data_editor(
                filter_df,
                column_config={
                    "対象": st.column_config.SelectboxColumn(
                        "対象",
                        options=ga4_filter_dims + dimensions,
                        required=True,
                    ),
                    "演算子": st.column_config.SelectboxColumn(
                        "演算子",
                        options=GA4_OPERATORS,
                        required=True,
                    ),
                    "値": st.column_config.TextColumn("値", required=True),
                },
                num_rows="dynamic",
                use_container_width=True,
                key="ga4_filter_editor"
            )
            
            # DataFrameから文字列に変換
            filter_d = serialize_ga4_filter_from_df(edited_filter_df)
            st.session_state["w_ga4_filter"] = filter_d
            
            if filter_d:
                st.caption(f"📝 `{filter_d}`")

    elif source == "GSC":
        # GSC設定
        sites = get_gsc_sites()

        # サイトURLの初期選択（keyを使って制御）
        if "w_gsc_site" not in st.session_state:
            loaded_site_url = lp.get("site_url", "")
            if loaded_site_url in sites:
                st.session_state["w_gsc_site"] = loaded_site_url
            elif sites:
                st.session_state["w_gsc_site"] = sites[0]

        site_url = st.selectbox("サイト", sites, key="w_gsc_site")

        # ディメンション（初期化）
        all_gsc_dims = ["query", "page", "country", "device", "date"]
        if "w_gsc_dimensions" not in st.session_state:
            st.session_state["w_gsc_dimensions"] = lp.get("dimensions", ["query"]) if lp.get("source", "").lower() == "gsc" else ["query"]
        dimensions = st.multiselect("ディメンション", all_gsc_dims, key="w_gsc_dimensions")
        
        # フィルタ（初期化）
        if "w_gsc_filter" not in st.session_state:
            st.session_state["w_gsc_filter"] = lp.get("filter", "") if lp.get("source", "").lower() == "gsc" else ""
        
        # フィルタをDataFrameにパース
        gsc_filter_df = parse_gsc_filter_to_df(st.session_state.get("w_gsc_filter", ""))
        
        with st.expander("フィルタ条件", expanded=bool(len(gsc_filter_df))):
            gsc_filter_dims = ["query", "page", "country", "device", "date"]
            
            edited_gsc_filter_df = st.data_editor(
                gsc_filter_df,
                column_config={
                    "対象": st.column_config.SelectboxColumn(
                        "対象",
                        options=gsc_filter_dims,
                        required=True,
                    ),
                    "演算子": st.column_config.SelectboxColumn(
                        "演算子",
                        options=GSC_OPERATORS,
                        required=True,
                    ),
                    "値": st.column_config.TextColumn("値", required=True),
                },
                num_rows="dynamic",
                use_container_width=True,
                key="gsc_filter_editor"
            )
            
            # DataFrameから文字列に変換
            gsc_filter = serialize_gsc_filter_from_df(edited_gsc_filter_df)
            st.session_state["w_gsc_filter"] = gsc_filter
            
            if gsc_filter:
                st.caption(f"📝 `{gsc_filter}`")

    else:
        # BigQuery設定
        if "w_bq_project" not in st.session_state:
            st.session_state["w_bq_project"] = lp.get("project_id", "")
        bq_project = st.text_input("プロジェクトID", key="w_bq_project")

    # 取得件数（BigQuery以外）
    if source != "BigQuery":
        if "w_limit" not in st.session_state:
            st.session_state["w_limit"] = lp.get("limit", 1000)
        # カンマ形式で選択肢を表示
        limit_options = [100, 500, 1000, 5000, 10000, 25000, 50000, 100000]
        limit_labels = {v: f"{v:,}" for v in limit_options}
        
        # 現在値が選択肢にない場合は最も近い値を選択
        current_limit = st.session_state.get("w_limit", 1000)
        if current_limit not in limit_options:
            current_limit = min(limit_options, key=lambda x: abs(x - current_limit))
        
        limit = st.select_slider(
            "取得件数",
            options=limit_options,
            value=current_limit,
            format_func=lambda x: limit_labels[x],
            key="w_limit"
        )

    st.divider()

    execute_btn = st.button("🚀 実行", type="primary", use_container_width=True)

# 自動実行チェック
auto_execute_pending = st.session_state.get("auto_execute_pending", False)
if auto_execute_pending:
    st.session_state["auto_execute_pending"] = False  # フラグをクリア

# BigQuery SQL入力エリア（メインエリアに表示）
if source == "BigQuery":
    st.subheader("SQL クエリ")
    
    # サンプルSQL
    sample_sql = """SELECT 
    event_date,
    COUNT(*) as event_count
FROM `project.analytics_123456789.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260101' AND '20260131'
GROUP BY event_date
ORDER BY event_date"""
    
    if "w_bq_sql" not in st.session_state:
        st.session_state["w_bq_sql"] = lp.get("sql", sample_sql) if lp.get("source", "").lower() == "bigquery" else sample_sql
    
    sql = st.text_area("SQL", height=200, key="w_bq_sql")
    
    # データセット一覧表示
    if bq_project:
        with st.expander("📁 データセット一覧"):
            try:
                datasets = get_bq_datasets(bq_project)
                if datasets:
                    st.write(", ".join(datasets))
                else:
                    st.info("データセットが見つかりません")
            except Exception as e:
                st.warning(f"データセット取得エラー: {e}")

# メインエリア
if execute_btn or auto_execute_pending or (file_just_updated and st.session_state.get("auto_execute", False)):
    with st.spinner("データを取得中..."):
        try:
            if source == "GA4":
                df = execute_ga4_query(
                    property_id,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    dimensions,
                    metrics,
                    filter_d,
                    limit
                )
            elif source == "GSC":
                gsc_dimension_filter = parse_gsc_filter(gsc_filter) if 'gsc_filter' in dir() else None
                df = execute_gsc_query(
                    site_url,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    dimensions,
                    limit,
                    gsc_dimension_filter
                )
            else:
                # BigQuery
                if not bq_project:
                    st.error("プロジェクトIDを入力してください")
                    df = None
                elif not sql.strip():
                    st.error("SQLを入力してください")
                    df = None
                else:
                    df = execute_bq_query(bq_project, sql)
            
            if df is not None and not df.empty:
                st.success(f"✓ {len(df):,} 行のデータを取得しました")
                st.session_state["df"] = df
            elif df is not None:
                st.warning("データが取得できませんでした")
                
        except Exception as e:
            st.error(f"エラー: {e}")

# 結果表示
if "df" in st.session_state:
    df = st.session_state["df"]
    
    # タブ
    tab1, tab2, tab3 = st.tabs(["📋 テーブル", "📈 チャート", "💾 保存"])
    
    with tab1:
        st.dataframe(df, use_container_width=True, height=400)
        
        # 統計情報
        with st.expander("統計情報"):
            st.write(df.describe())
    
    with tab2:
        if len(df.columns) >= 2:
            col1, col2 = st.columns(2)
            with col1:
                x_col = st.selectbox("X軸", df.columns)
            with col2:
                y_col = st.selectbox("Y軸", [c for c in df.columns if c != x_col])
            
            chart_type = st.radio("チャートタイプ", ["折れ線", "棒グラフ"], horizontal=True)
            
            if chart_type == "折れ線":
                st.line_chart(df.set_index(x_col)[y_col])
            else:
                st.bar_chart(df.set_index(x_col)[y_col])
    
    with tab3:
        st.subheader("ローカル保存")
        col1, col2 = st.columns(2)
        with col1:
            # CSV ダウンロード
            csv = df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 CSV ダウンロード",
                csv,
                f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv",
                use_container_width=True
            )
        with col2:
            # ファイル保存
            if st.button("💾 output/ に保存", use_container_width=True):
                import os
                os.makedirs("output", exist_ok=True)
                filepath = f"output/result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
                st.success(f"保存しました: {filepath}")
        
        st.divider()
        st.subheader("Google Sheets に保存")
        
        # スプレッドシートURL
        sheet_url = st.text_input(
            "スプレッドシートURL",
            placeholder="https://docs.google.com/spreadsheets/d/xxxxx",
            key="w_sheet_url"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            sheet_name = st.text_input("シート名", value="data", key="w_sheet_name")
        with col2:
            save_mode = st.selectbox("保存モード", ["上書き", "追記", "アップサート"], key="w_save_mode")
        
        # アップサート時のキー列
        if save_mode == "アップサート":
            key_cols = st.multiselect("キー列", df.columns.tolist(), key="w_upsert_keys")
        
        if st.button("📤 Google Sheets に保存", use_container_width=True, type="primary"):
            if not sheet_url:
                st.error("スプレッドシートURLを入力してください")
            else:
                try:
                    mode_map = {"上書き": "overwrite", "追記": "append", "アップサート": "upsert"}
                    mode = mode_map[save_mode]
                    
                    if mode == "upsert" and not key_cols:
                        st.error("キー列を選択してください")
                    else:
                        save_to_sheet(sheet_url, sheet_name, df, mode=mode, keys=key_cols if mode == "upsert" else None)
                        st.success(f"✓ シート「{sheet_name}」に保存しました")
                except Exception as e:
                    st.error(f"エラー: {e}")

# JSONパラメータ表示（AI Agent連携用）
with st.sidebar:
    with st.expander("🤖 JSON (AI Agent用)"):
        if source == "GA4":
            params = {
                "source": "ga4",
                "property_id": property_id if 'property_id' in dir() else "",
                "date_range": {
                    "start": start_date.strftime("%Y-%m-%d"),
                    "end": end_date.strftime("%Y-%m-%d")
                },
                "dimensions": dimensions if 'dimensions' in dir() else [],
                "metrics": metrics if 'metrics' in dir() else [],
                "filter_d": filter_d if 'filter_d' in dir() else "",
                "limit": limit
            }
        elif source == "GSC":
            params = {
                "source": "gsc",
                "site_url": site_url if 'site_url' in dir() else "",
                "date_range": {
                    "start": start_date.strftime("%Y-%m-%d"),
                    "end": end_date.strftime("%Y-%m-%d")
                },
                "dimensions": dimensions if 'dimensions' in dir() else [],
                "filter": gsc_filter if 'gsc_filter' in dir() else "",
                "limit": limit
            }
        else:
            params = {
                "source": "bigquery",
                "project_id": bq_project if 'bq_project' in dir() else "",
                "sql": sql if 'sql' in dir() else ""
            }
        st.code(json.dumps(params, indent=2, ensure_ascii=False), language="json")
