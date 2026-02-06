"""Streamlit UI メインアプリ"""
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

st.set_page_config(
    page_title="GA4/GSC 分析ツール",
    page_icon="📊",
    layout="wide",
)

# === パラメータファイル監視 ===

PARAMS_FILE = Path("input/params.json")

def load_params_from_file():
    """外部JSONファイルからパラメータを読み込む"""
    if not PARAMS_FILE.exists():
        return None, None
    try:
        mtime = PARAMS_FILE.stat().st_mtime
        with open(PARAMS_FILE, "r", encoding="utf-8") as f:
            params = json.load(f)
        return params, mtime
    except (json.JSONDecodeError, IOError):
        return None, None

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
        if "metrics" in params:
            st.session_state["w_ga4_metrics"] = params["metrics"]
        if "filter_d" in params:
            st.session_state["w_ga4_filter"] = params["filter_d"]

    # GSC固有
    if source == "gsc":
        if "site_url" in params:
            st.session_state["w_gsc_site"] = params["site_url"]

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

# === キャッシュ付きデータ取得 ===

@st.cache_resource
def get_megaton():
    """megatonインスタンスを取得（キャッシュ）"""
    from megaton import start
    return start.Megaton("credentials/sa-shibuya-kyousei.json", headless=True)

@st.cache_data(ttl=300)
def get_ga4_properties():
    """GA4プロパティ一覧を取得"""
    mg = get_megaton()
    result = []
    for acc in mg.ga["4"].accounts:
        for prop in acc.get("properties", []):
            result.append({
                "id": prop["id"],
                "name": prop["name"],
                "display": f"{prop['name']} ({prop['id']})"
            })
    return result

@st.cache_data(ttl=300)
def get_gsc_sites():
    """GSCサイト一覧を取得"""
    mg = get_megaton()
    return mg.search.get.sites()

@st.cache_data(ttl=60)
def execute_ga4_query(property_id, start_date, end_date, dimensions, metrics, filter_d, limit):
    """GA4クエリを実行"""
    mg = get_megaton()
    
    # プロパティ選択
    for acc in mg.ga["4"].accounts:
        for prop in acc.get("properties", []):
            if prop["id"] == property_id:
                mg.ga["4"].account.select(acc["id"])
                mg.ga["4"].property.select(property_id)
                break
    
    mg.report.set.dates(start_date, end_date)
    mg.report.run(d=dimensions, m=metrics, filter_d=filter_d if filter_d else None, limit=limit, show=False)
    return mg.report.data

@st.cache_data(ttl=60)
def execute_gsc_query(site_url, start_date, end_date, dimensions, limit):
    """GSCクエリを実行"""
    mg = get_megaton()
    mg.search.use(site_url)
    mg.search.set.dates(start_date, end_date)
    mg.search.run(dimensions=dimensions, metrics=["clicks", "impressions", "ctr", "position"], limit=limit)
    return mg.search.data


# === UI ===

st.title("📊 GA4 / Search Console 分析ツール")

# === ファイル監視セクション ===

# セッション初期化
if "auto_watch" not in st.session_state:
    st.session_state["auto_watch"] = True
if "auto_execute" not in st.session_state:
    st.session_state["auto_execute"] = False

# 自動リフレッシュ（ファイル監視用：2秒ごと）
if st.session_state.get("auto_watch", True):
    st_autorefresh(interval=2000, limit=None, key="file_watcher_refresh")

# ファイル変更チェック用フラグ
file_just_updated = False

# ファイル変更チェック（メインスクリプト内で実行）
if st.session_state.get("auto_watch", True) and check_file_updated():
    params, _ = load_params_from_file()
    if params:
        apply_params_to_session(params)
        st.toast("🔄 パラメータファイルが更新されました", icon="📄")
        file_just_updated = True
        if st.session_state.get("auto_execute", False):
            st.session_state["auto_execute_pending"] = True

with st.sidebar:
    with st.expander("🤖 AI Agent 連携", expanded=True):
        col1, col2 = st.columns([3, 1])
        with col1:
            st.session_state["auto_watch"] = st.toggle(
                "ファイル自動監視",
                value=st.session_state.get("auto_watch", True),
                help="input/params.json の変更を2秒ごとに検知"
            )
        with col2:
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
        if st.button("📥 ファイルから読み込み", use_container_width=True):
            params, mtime = load_params_from_file()
            if params:
                apply_params_to_session(params)
                st.session_state["last_params_mtime"] = mtime
                st.success("✓ パラメータを読み込みました")
                st.rerun()
            else:
                st.warning("params.json が見つかりません")

# サイドバー
with st.sidebar:
    st.header("設定")

    # 読み込み済みパラメータを取得
    lp = st.session_state.get("loaded_params", {})

    # パラメータ反映時の通知
    if st.session_state.get("params_applied"):
        st.info("📥 ファイルからパラメータを反映しました")
        st.session_state["params_applied"] = False

    # データソース選択
    default_source = "GA4" if lp.get("source", "ga4").lower() == "ga4" else "GSC"
    source = st.radio("データソース", ["GA4", "GSC"], horizontal=True,
                      index=0 if default_source == "GA4" else 1)

    st.divider()

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

        # プロパティIDからdisplay名を逆引き
        default_prop_idx = 0
        loaded_prop_id = lp.get("property_id", "")
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
        filter_d = st.text_input(
            "フィルタ (例: sessionDefaultChannelGroup==Organic Search)",
            key="w_ga4_filter"
        )

    else:
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

    # 取得件数（初期化）
    if "w_limit" not in st.session_state:
        st.session_state["w_limit"] = lp.get("limit", 1000)
    limit = st.slider("取得件数", 10, 5000, key="w_limit")

    st.divider()

    execute_btn = st.button("🚀 実行", type="primary", use_container_width=True)

# 自動実行チェック
auto_execute_pending = st.session_state.get("auto_execute_pending", False)
if auto_execute_pending:
    st.session_state["auto_execute_pending"] = False  # フラグをクリア

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
            else:
                df = execute_gsc_query(
                    site_url,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    dimensions,
                    limit
                )
            
            if df is not None and not df.empty:
                st.success(f"✓ {len(df):,} 行のデータを取得しました")
                st.session_state["df"] = df
            else:
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
        else:
            params = {
                "source": "gsc",
                "site_url": site_url if 'site_url' in dir() else "",
                "date_range": {
                    "start": start_date.strftime("%Y-%m-%d"),
                    "end": end_date.strftime("%Y-%m-%d")
                },
                "dimensions": dimensions if 'dimensions' in dir() else [],
                "limit": limit
            }
        st.code(json.dumps(params, indent=2, ensure_ascii=False), language="json")
