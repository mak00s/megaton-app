"""Streamlit UI メインアプリ"""
import streamlit as st
import pandas as pd
import json
from datetime import datetime, timedelta

st.set_page_config(
    page_title="GA4/GSC 分析ツール",
    page_icon="📊",
    layout="wide",
)

# === キャッシュ付きデータ取得 ===

@st.cache_resource
def get_megaton():
    """megatonインスタンスを取得（キャッシュ）"""
    from megaton import start
    return start.Megaton("credentials/sa-example-site-kyousei.json", headless=True)

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

# サイドバー
with st.sidebar:
    st.header("設定")
    
    # データソース選択
    source = st.radio("データソース", ["GA4", "GSC"], horizontal=True)
    
    st.divider()
    
    # 日付範囲
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "開始日",
            value=datetime.now() - timedelta(days=14)
        )
    with col2:
        end_date = st.date_input(
            "終了日",
            value=datetime.now() - timedelta(days=1)
        )
    
    st.divider()
    
    if source == "GA4":
        # GA4設定
        properties = get_ga4_properties()
        property_options = {p["display"]: p["id"] for p in properties}
        selected_property = st.selectbox("プロパティ", list(property_options.keys()))
        property_id = property_options[selected_property]
        
        dimensions = st.multiselect(
            "ディメンション",
            ["date", "sessionDefaultChannelGroup", "sessionSource", "sessionMedium", 
             "pagePath", "landingPage", "deviceCategory", "country"],
            default=["date"]
        )
        
        metrics = st.multiselect(
            "メトリクス",
            ["sessions", "activeUsers", "newUsers", "screenPageViews", 
             "bounceRate", "averageSessionDuration", "conversions"],
            default=["sessions", "activeUsers"]
        )
        
        filter_d = st.text_input(
            "フィルタ (例: sessionDefaultChannelGroup==Organic Search)",
            value=""
        )
        
    else:
        # GSC設定
        sites = get_gsc_sites()
        site_url = st.selectbox("サイト", sites)
        
        dimensions = st.multiselect(
            "ディメンション",
            ["query", "page", "country", "device", "date"],
            default=["query"]
        )
    
    limit = st.slider("取得件数", 10, 5000, 1000)
    
    st.divider()
    
    execute_btn = st.button("🚀 実行", type="primary", use_container_width=True)

# メインエリア
if execute_btn:
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
