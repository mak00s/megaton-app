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
        return None, None, [], None
    try:
        mtime = PARAMS_FILE.stat().st_mtime
        with open(PARAMS_FILE, "r", encoding="utf-8") as f:
            raw_params = json.load(f)
        canonical = canonicalize_json(raw_params)
        params, errors = validate_params(raw_params)
        return params, mtime, errors, canonical
    except json.JSONDecodeError as e:
        return None, None, [{
            "error_code": "INVALID_JSON",
            "message": f"Invalid JSON: {e}",
            "path": "$",
            "hint": "Fix JSON syntax in input/params.json."
        }], None
    except IOError as e:
        return None, None, [{
            "error_code": "FILE_IO_ERROR",
            "message": f"Failed to read params.json: {e}",
            "path": "$",
            "hint": "Check file permissions and file path."
        }], None

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

    # パイプライン（未指定項目を残さないよう毎回初期化してから反映）
    pipeline = params.get("pipeline") or {}
    st.session_state["w_tf_date"] = False
    st.session_state["w_tf_url_decode"] = False
    st.session_state["w_tf_strip_qs"] = False
    st.session_state["w_tf_keep_params"] = ""
    st.session_state["w_tf_path_only"] = False
    st.session_state["w_pipeline_where"] = ""
    st.session_state["w_pipeline_columns"] = []
    st.session_state["w_pipeline_group_by"] = []
    st.session_state["w_pipeline_head"] = 0
    for key in list(st.session_state.keys()):
        if key.startswith("w_agg_"):
            del st.session_state[key]

    if pipeline:
        if pipeline.get("transform"):
            expr = pipeline["transform"]
            try:
                transforms = parse_transforms(expr)
            except ValueError:
                transforms = []
            for _, func, args in transforms:
                if func == "date_format":
                    st.session_state["w_tf_date"] = True
                elif func == "url_decode":
                    st.session_state["w_tf_url_decode"] = True
                elif func == "strip_qs":
                    st.session_state["w_tf_strip_qs"] = True
                    if args:
                        st.session_state["w_tf_keep_params"] = args
                elif func == "path_only":
                    st.session_state["w_tf_path_only"] = True
        if pipeline.get("where"):
            st.session_state["w_pipeline_where"] = pipeline["where"]
        if pipeline.get("columns"):
            st.session_state["w_pipeline_columns"] = [
                c.strip() for c in pipeline["columns"].split(",") if c.strip()
            ]
        if pipeline.get("group_by"):
            st.session_state["w_pipeline_group_by"] = [
                c.strip() for c in pipeline["group_by"].split(",") if c.strip()
            ]
        if pipeline.get("aggregate"):
            for part in pipeline["aggregate"].split(","):
                tokens = [x.strip() for x in part.split(":", 1)]
                if len(tokens) == 2 and tokens[0] and tokens[1]:
                    func, col = tokens
                    st.session_state[f"w_agg_{col}"] = func
        if pipeline.get("head") is not None:
            st.session_state["w_pipeline_head"] = pipeline["head"]

    # save
    save = params.get("save") or {}
    # 初期化
    st.session_state["w_sheet_url"] = ""
    st.session_state["w_sheet_name"] = "data"
    st.session_state["w_save_mode"] = "上書き"
    st.session_state["w_save_bq_project"] = ""
    st.session_state["w_save_bq_dataset"] = ""
    st.session_state["w_save_bq_table"] = ""
    st.session_state["w_save_bq_mode"] = "上書き"
    st.session_state["w_save_filename"] = ""

    if save:
        mode_rmap = {"overwrite": "上書き", "append": "追記", "upsert": "アップサート"}
        target = save.get("to")

        if target == "csv":
            path = save.get("path", "")
            if path:
                st.session_state["w_save_filename"] = Path(path).name

        elif target == "sheets":
            st.session_state["w_sheet_url"] = save.get("sheet_url", "")
            st.session_state["w_sheet_name"] = save.get("sheet_name", "data")
            st.session_state["w_save_mode"] = mode_rmap.get(save.get("mode", "overwrite"), "上書き")
            if save.get("keys"):
                st.session_state["w_upsert_keys"] = save["keys"]

        elif target == "bigquery":
            st.session_state["w_save_bq_project"] = save.get("project_id", "")
            st.session_state["w_save_bq_dataset"] = save.get("dataset", "")
            st.session_state["w_save_bq_table"] = save.get("table", "")
            st.session_state["w_save_bq_mode"] = mode_rmap.get(save.get("mode", "overwrite"), "上書き")

    return True

def check_file_updated():
    """ファイル更新をチェック（mtime + 実質差分）"""
    if not PARAMS_FILE.exists():
        return False, None, None, []

    current_mtime = PARAMS_FILE.stat().st_mtime
    last_mtime = st.session_state.get("last_params_mtime", 0)

    if current_mtime <= last_mtime:
        return False, None, None, []

    params, mtime, errors, canonical = load_params_from_file()
    st.session_state["last_params_mtime"] = current_mtime

    last_canonical = st.session_state.get("last_params_canonical")
    if not has_effective_params_update(current_mtime, last_mtime, canonical, last_canonical):
        return False, None, None, []

    st.session_state["last_params_canonical"] = canonical
    return True, params, mtime, errors

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
    save_to_bq,
)
from lib.params_diff import canonicalize_json
from lib.params_validator import validate_params
from lib.result_inspector import apply_pipeline, SUPPORTED_AGG_FUNCS, parse_transforms
from app.ui.params_utils import (
    GA4_OPERATORS,
    GSC_OPERATORS,
    parse_ga4_filter_to_df,
    serialize_ga4_filter_from_df,
    parse_gsc_filter_to_df,
    serialize_gsc_filter_from_df,
    has_effective_params_update,
)

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
if "last_params_canonical" not in st.session_state:
    st.session_state["last_params_canonical"] = None

# 自動リフレッシュ（ファイル監視用：2秒ごと）
if st.session_state.get("auto_watch", True):
    st_autorefresh(interval=2000, limit=None, key="file_watcher_refresh")

# ファイル変更チェック用フラグ
file_just_updated = False

# ファイル変更チェック（メインスクリプト内で実行）
if st.session_state.get("auto_watch", True):
    updated, params, _, errors = check_file_updated()
    if updated:
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
        if st.button("📥 JSONを開く", width="stretch"):
            params, mtime, errors, canonical = load_params_from_file()
            if params:
                apply_params_to_session(params)
                st.session_state["last_params_mtime"] = mtime
                st.session_state["last_params_canonical"] = canonical
                st.session_state["params_validation_errors"] = []
                st.success("✓ パラメータを読み込みました")
                st.rerun()
            elif errors:
                if canonical is not None:
                    st.session_state["last_params_canonical"] = canonical
                if mtime is not None:
                    st.session_state["last_params_mtime"] = mtime
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
        try:
            properties = get_ga4_properties()
        except (RuntimeError, FileNotFoundError, ValueError) as e:
            st.error(f"⚠️ 認証エラー: {e}")
            st.stop()
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
                width="stretch",
                key="ga4_filter_editor"
            )
            
            # DataFrameから文字列に変換
            filter_d = serialize_ga4_filter_from_df(edited_filter_df)
            st.session_state["w_ga4_filter"] = filter_d
            
            if filter_d:
                st.caption(f"📝 `{filter_d}`")

    elif source == "GSC":
        # GSC設定
        try:
            sites = get_gsc_sites()
        except (RuntimeError, FileNotFoundError, ValueError) as e:
            st.error(f"⚠️ 認証エラー: {e}")
            st.stop()

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
                width="stretch",
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

    execute_btn = st.button("🚀 実行", type="primary", width="stretch")

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
    raw_df = st.session_state["df"]

    # === パイプラインUI ===
    with st.expander("🔧 結果の絞り込み・集計", expanded=False):
        pipeline_kwargs = {}

        # --- 変換 ---
        st.markdown("**変換**")
        transform_parts = []

        has_date_col = "date" in raw_df.columns
        # URL列があるか判定（値が http で始まる文字列列を探す）
        url_cols = []
        for c in raw_df.select_dtypes(include="object").columns:
            sample = raw_df[c].dropna().head(5).astype(str)
            if sample.str.startswith("http").any():
                url_cols.append(c)
        has_url_col = len(url_cols) > 0

        pcol1, pcol2 = st.columns(2)
        with pcol1:
            tf_date = st.checkbox(
                "日付を YYYY-MM-DD に変換",
                disabled=not has_date_col,
                key="w_tf_date",
            )
            if tf_date and has_date_col:
                transform_parts.append("date:date_format")

            tf_url_decode = st.checkbox(
                "URLデコード",
                disabled=not has_url_col,
                key="w_tf_url_decode",
            )
            if tf_url_decode and url_cols:
                for uc in url_cols:
                    transform_parts.append(f"{uc}:url_decode")

        with pcol2:
            tf_strip_qs = st.checkbox(
                "クエリ文字列を除去",
                disabled=not has_url_col,
                key="w_tf_strip_qs",
            )
            if tf_strip_qs and url_cols:
                keep_params = st.text_input(
                    "残すパラメータ（カンマ区切り、空=全除去）",
                    key="w_tf_keep_params",
                    placeholder="id,ref",
                )
                for uc in url_cols:
                    if keep_params.strip():
                        transform_parts.append(f"{uc}:strip_qs:{keep_params.strip()}")
                    else:
                        transform_parts.append(f"{uc}:strip_qs")

            tf_path_only = st.checkbox(
                "パスのみ（ドメイン除去）",
                disabled=not has_url_col,
                key="w_tf_path_only",
            )
            if tf_path_only and url_cols:
                for uc in url_cols:
                    transform_parts.append(f"{uc}:path_only")

        if transform_parts:
            pipeline_kwargs["transform"] = ",".join(transform_parts)

        st.divider()

        # --- フィルタ ---
        st.markdown("**フィルタ**")
        where_expr = st.text_input(
            "条件式（pandas query構文）",
            key="w_pipeline_where",
            placeholder='clicks > 100 and page.str.contains("/blog/")',
        )
        if where_expr.strip():
            pipeline_kwargs["where"] = where_expr.strip()

        st.divider()

        # --- 表示列 ---
        st.markdown("**表示列**")
        selected_cols = st.multiselect(
            "列を選択（空=全列）",
            list(raw_df.columns),
            key="w_pipeline_columns",
        )
        if selected_cols:
            pipeline_kwargs["columns"] = ",".join(selected_cols)

        st.divider()

        # --- グループ集計 ---
        st.markdown("**グループ集計**")
        group_cols = st.multiselect(
            "グループ列",
            list(raw_df.columns),
            key="w_pipeline_group_by",
        )
        numeric_cols = list(raw_df.select_dtypes(include="number").columns)
        agg_exprs = []
        if group_cols and numeric_cols:
            st.caption("集計関数を設定")
            for nc in numeric_cols:
                agg_func = st.selectbox(
                    f"{nc}",
                    ["（なし）", "sum", "mean", "count", "min", "max", "median"],
                    key=f"w_agg_{nc}",
                )
                if agg_func != "（なし）":
                    agg_exprs.append(f"{agg_func}:{nc}")

        if group_cols and agg_exprs:
            pipeline_kwargs["group_by"] = ",".join(group_cols)
            pipeline_kwargs["aggregate"] = ",".join(agg_exprs)

            # グループ集計後はソート列名が変わるため更新
            # sum_clicks のような列名でソートしたい場合があるので案内
            derived_cols = [f"{a.split(':')[0]}_{a.split(':')[1]}" for a in agg_exprs]
            st.caption(f"集計後の列: {', '.join(group_cols + derived_cols)}")

        st.divider()

        # --- 表示行数 ---
        st.markdown("**表示行数**")
        head_val = st.slider(
            "先頭N行（0=全行）",
            min_value=0,
            max_value=min(len(raw_df), 10000),
            value=0,
            step=10,
            key="w_pipeline_head",
        )
        if head_val > 0:
            pipeline_kwargs["head"] = head_val

    # === パイプライン適用 ===
    pipeline_error = None
    if pipeline_kwargs:
        try:
            display_df = apply_pipeline(raw_df, **pipeline_kwargs)
        except ValueError as e:
            pipeline_error = str(e)
            display_df = raw_df
    else:
        display_df = raw_df

    if pipeline_error:
        st.error(f"パイプラインエラー: {pipeline_error}")

    # 行数キャプション
    if len(display_df) != len(raw_df):
        st.caption(f"📊 {len(raw_df):,} 行 → {len(display_df):,} 行")
    else:
        st.caption(f"📊 {len(display_df):,} 行")

    # タブ
    tab1, tab2, tab3 = st.tabs(["📋 テーブル", "📈 チャート", "💾 保存"])

    with tab1:
        st.dataframe(display_df, width="stretch", height=400)

        # 統計情報
        with st.expander("統計情報"):
            st.write(display_df.describe())

    with tab2:
        if len(display_df.columns) >= 2:
            col1, col2 = st.columns(2)
            with col1:
                x_col = st.selectbox("X軸", display_df.columns)
            with col2:
                y_col = st.selectbox("Y軸", [c for c in display_df.columns if c != x_col])

            chart_type = st.radio("チャートタイプ", ["折れ線", "棒グラフ"], horizontal=True)

            if chart_type == "折れ線":
                st.line_chart(display_df.set_index(x_col)[y_col])
            else:
                st.bar_chart(display_df.set_index(x_col)[y_col])

    with tab3:
        st.subheader("ローカル保存")
        save_filename = st.text_input(
            "ファイル名",
            value=f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            key="w_save_filename",
        )
        col1, col2 = st.columns(2)
        with col1:
            # CSV ダウンロード
            csv = display_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 CSV ダウンロード",
                csv,
                save_filename,
                "text/csv",
                width="stretch"
            )
        with col2:
            # ファイル保存
            if st.button("💾 output/ に保存", width="stretch"):
                os.makedirs("output", exist_ok=True)
                filepath = f"output/{save_filename}"
                display_df.to_csv(filepath, index=False, encoding='utf-8-sig')
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
            key_cols = st.multiselect("キー列", display_df.columns.tolist(), key="w_upsert_keys")

        if st.button("📤 Google Sheets に保存", width="stretch", type="primary"):
            if not sheet_url:
                st.error("スプレッドシートURLを入力してください")
            else:
                try:
                    mode_map = {"上書き": "overwrite", "追記": "append", "アップサート": "upsert"}
                    mode = mode_map[save_mode]

                    if mode == "upsert" and not key_cols:
                        st.error("キー列を選択してください")
                    else:
                        save_to_sheet(sheet_url, sheet_name, display_df, mode=mode, keys=key_cols if mode == "upsert" else None)
                        st.success(f"✓ シート「{sheet_name}」に保存しました")
                except Exception as e:
                    st.error(f"エラー: {e}")

        st.divider()
        st.subheader("BigQuery に保存")

        bq_project = st.text_input(
            "GCPプロジェクトID",
            key="w_save_bq_project",
            placeholder="my-project-id",
        )
        col1, col2 = st.columns(2)
        with col1:
            bq_dataset = st.text_input("データセット", key="w_save_bq_dataset")
        with col2:
            bq_table = st.text_input("テーブル", key="w_save_bq_table")

        bq_mode = st.selectbox("保存モード", ["上書き", "追記"], key="w_save_bq_mode")

        if st.button("📤 BigQuery に保存", width="stretch", type="primary"):
            if not all([bq_project, bq_dataset, bq_table]):
                st.error("プロジェクトID、データセット、テーブルを入力してください")
            else:
                try:
                    bq_mode_map = {"上書き": "overwrite", "追記": "append"}
                    save_to_bq(bq_project, bq_dataset, bq_table, display_df, mode=bq_mode_map[bq_mode])
                    st.success(f"✓ {bq_project}.{bq_dataset}.{bq_table} に保存しました")
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
