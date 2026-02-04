"""Gradio UI メインアプリ"""
import gradio as gr
import pandas as pd
import json
import sys
import os
from datetime import datetime, timedelta

# パス追加（app/ からの相対インポート用）
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.schemas import QueryParams, SAMPLE_GA4_JSON, SAMPLE_GSC_JSON

# 遅延インポート用（重いモジュールの初期化を実行時まで遅延）
_ga4_module = None
_gsc_module = None
_viz_module = None

# キャッシュ
_properties_cache = None
_sites_cache = None
_last_result_df = None  # 最後の実行結果を保持

# 出力ディレクトリ
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def _get_ga4_module():
    global _ga4_module
    if _ga4_module is None:
        from app.engine import ga4
        _ga4_module = ga4
    return _ga4_module

def _get_gsc_module():
    global _gsc_module
    if _gsc_module is None:
        from app.engine import gsc
        _gsc_module = gsc
    return _gsc_module

def _get_viz_module():
    global _viz_module
    if _viz_module is None:
        from app.engine import visualize
        _viz_module = visualize
    return _viz_module


def execute_query(json_params: str):
    """JSONパラメータを実行してテーブルとチャートを返す"""
    global _last_result_df
    
    try:
        params = QueryParams.from_json(json_params)
        
        # データ取得（遅延インポート）
        if params.source == "ga4":
            ga4 = _get_ga4_module()
            df = ga4.execute_ga4_query(params)
        elif params.source == "gsc":
            gsc = _get_gsc_module()
            df = gsc.execute_gsc_query(params)
        else:
            return None, None, f"不明なソース: {params.source}"
        
        if df is None or df.empty:
            _last_result_df = None
            return None, None, "データが取得できませんでした"
        
        # 結果を保持
        _last_result_df = df.copy()
        
        viz = _get_viz_module()
        
        # チャート生成
        chart = None
        if params.visualization:
            chart = viz.create_chart(df, params.visualization)
        
        # テーブル用にフォーマット
        df_display = viz.format_dataframe(df)
        
        return df_display, chart, f"✓ {len(df)} 行のデータを取得しました"
        
    except json.JSONDecodeError as e:
        _last_result_df = None
        return None, None, f"JSONパースエラー: {e}"
    except Exception as e:
        _last_result_df = None
        return None, None, f"エラー: {e}"


def save_to_csv():
    """結果をCSVに保存"""
    global _last_result_df
    
    if _last_result_df is None:
        return None, "保存するデータがありません"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"result_{timestamp}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    _last_result_df.to_csv(filepath, index=False, encoding="utf-8-sig")
    
    return filepath, f"✓ 保存完了: {filepath}"


def load_ga4_sample():
    """GA4 サンプルJSONを読み込み"""
    return SAMPLE_GA4_JSON


def load_gsc_sample():
    """GSC サンプルJSONを読み込み"""
    return SAMPLE_GSC_JSON


def get_properties_list():
    """GA4 プロパティ一覧を取得"""
    try:
        ga4 = _get_ga4_module()
        props = ga4.list_ga4_properties()
        return "\n".join([f"- {p['property_name']} (ID: {p['property_id']})" for p in props])
    except Exception as e:
        return f"エラー: {e}"


def get_sites_list():
    """GSC サイト一覧を取得"""
    try:
        gsc = _get_gsc_module()
        sites = gsc.list_gsc_sites()
        return "\n".join([f"- {s}" for s in sites])
    except Exception as e:
        return f"エラー: {e}"


def get_property_choices():
    """GA4プロパティ選択肢を取得"""
    global _properties_cache
    if _properties_cache is None:
        try:
            ga4 = _get_ga4_module()
            props = ga4.list_ga4_properties()
            _properties_cache = [(f"{p['property_name']} ({p['property_id']})", p['property_id']) for p in props]
        except Exception:
            _properties_cache = [("サンプルサイト - GA4 (123456789)", "123456789")]
    return _properties_cache


def get_site_choices():
    """GSCサイト選択肢を取得"""
    global _sites_cache
    if _sites_cache is None:
        try:
            gsc = _get_gsc_module()
            sites = gsc.list_gsc_sites()
            _sites_cache = [(s, s) for s in sites]
        except Exception:
            _sites_cache = [("https://www.example-sitekyousei.jp/", "https://www.example-sitekyousei.jp/")]
    return _sites_cache


def update_json_from_ui(json_str, source, property_id, site_url, start_date, end_date):
    """UIの選択値をJSONに反映"""
    try:
        data = json.loads(json_str)
        
        # ソース
        data["source"] = source
        
        # プロパティ/サイト
        if source == "ga4":
            data["property_id"] = property_id
            data.pop("site_url", None)
        else:
            data["site_url"] = site_url
            data.pop("property_id", None)
        
        # 日付
        data["date_range"] = {
            "start": start_date,
            "end": end_date
        }
        
        return json.dumps(data, indent=2, ensure_ascii=False)
    except Exception:
        return json_str


def on_source_change(source, json_str):
    """ソース変更時にサンプルJSONを切り替え"""
    if source == "ga4":
        return SAMPLE_GA4_JSON, gr.update(visible=True), gr.update(visible=False)
    else:
        return SAMPLE_GSC_JSON, gr.update(visible=False), gr.update(visible=True)


def sync_ui_from_json(json_str):
    """JSONからUIに値を反映"""
    try:
        data = json.loads(json_str)
        
        source = data.get("source", "ga4")
        property_id = data.get("property_id", "123456789")
        site_url = data.get("site_url", "https://www.example-sitekyousei.jp/")
        start = data.get("date_range", {}).get("start", DEFAULT_START_DATE)
        end = data.get("date_range", {}).get("end", DEFAULT_END_DATE)
        
        prop_visible = source == "ga4"
        site_visible = source == "gsc"
        
        return (
            source,
            property_id,
            site_url,
            start,
            end,
            gr.update(visible=prop_visible),
            gr.update(visible=site_visible),
        )
    except Exception:
        return (
            "ga4",
            "123456789",
            "https://www.example-sitekyousei.jp/",
            DEFAULT_START_DATE,
            DEFAULT_END_DATE,
            gr.update(visible=True),
            gr.update(visible=False),
        )


# デフォルト日付
DEFAULT_END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
DEFAULT_START_DATE = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


# Gradio UI
with gr.Blocks(title="GA4/GSC 分析ツール") as app:
    gr.Markdown("# GA4 / Search Console 分析ツール")
    
    with gr.Row():
        with gr.Column(scale=1):
            gr.Markdown("### 設定")
            
            # ソース選択
            source_radio = gr.Radio(
                choices=["ga4", "gsc"],
                value="ga4",
                label="データソース",
            )
            
            # GA4プロパティ選択
            property_dropdown = gr.Dropdown(
                choices=[("サンプルサイト - GA4 (123456789)", "123456789")],
                value="123456789",
                label="GA4 プロパティ",
                visible=True,
            )
            
            # GSCサイト選択
            site_dropdown = gr.Dropdown(
                choices=[("https://www.example-sitekyousei.jp/", "https://www.example-sitekyousei.jp/")],
                value="https://www.example-sitekyousei.jp/",
                label="Search Console サイト",
                visible=False,
            )
            
            # 日付範囲
            with gr.Row():
                start_date = gr.Textbox(
                    label="開始日",
                    value=DEFAULT_START_DATE,
                    placeholder="YYYY-MM-DD",
                )
                end_date = gr.Textbox(
                    label="終了日",
                    value=DEFAULT_END_DATE,
                    placeholder="YYYY-MM-DD",
                )
            
            # 同期ボタン
            with gr.Row():
                apply_btn = gr.Button("↓ JSONに反映", size="sm")
                load_btn = gr.Button("↑ UIに読み込み", size="sm")
            
            gr.Markdown("### JSONパラメータ")
            json_input = gr.Code(
                label="",
                language="json",
                lines=15,
                value=SAMPLE_GA4_JSON,
            )
            
            with gr.Row():
                ga4_sample_btn = gr.Button("GA4 サンプル", size="sm")
                gsc_sample_btn = gr.Button("GSC サンプル", size="sm")
            
            execute_btn = gr.Button("実行", variant="primary", size="lg")
            status_text = gr.Textbox(label="ステータス", interactive=False)
        
        with gr.Column(scale=2):
            gr.Markdown("### 結果")
            
            with gr.Tabs():
                with gr.TabItem("テーブル"):
                    result_table = gr.Dataframe(
                        label="データ",
                        interactive=False,
                        wrap=True,
                    )
                
                with gr.TabItem("チャート"):
                    result_chart = gr.Plot(label="グラフ")
            
            with gr.Row():
                save_csv_btn = gr.Button("📁 CSV保存", size="sm")
                csv_file = gr.File(label="ダウンロード", visible=False)
    
    # 起動時にプロパティ/サイト一覧を読み込み
    def load_choices():
        props = get_property_choices()
        sites = get_site_choices()
        return gr.update(choices=props), gr.update(choices=sites)
    
    app.load(load_choices, outputs=[property_dropdown, site_dropdown])
    
    # ソース変更時
    source_radio.change(
        on_source_change,
        inputs=[source_radio, json_input],
        outputs=[json_input, property_dropdown, site_dropdown],
    )
    
    # UI → JSON
    apply_btn.click(
        update_json_from_ui,
        inputs=[json_input, source_radio, property_dropdown, site_dropdown, start_date, end_date],
        outputs=json_input,
    )
    
    # JSON → UI
    load_btn.click(
        sync_ui_from_json,
        inputs=json_input,
        outputs=[source_radio, property_dropdown, site_dropdown, start_date, end_date, property_dropdown, site_dropdown],
    )
    
    # サンプル読み込み（UIにも反映）
    def load_ga4_and_sync():
        json_str = SAMPLE_GA4_JSON
        return (json_str,) + sync_ui_from_json(json_str)
    
    def load_gsc_and_sync():
        json_str = SAMPLE_GSC_JSON
        return (json_str,) + sync_ui_from_json(json_str)
    
    ga4_sample_btn.click(
        load_ga4_and_sync,
        outputs=[json_input, source_radio, property_dropdown, site_dropdown, start_date, end_date, property_dropdown, site_dropdown],
    )
    gsc_sample_btn.click(
        load_gsc_and_sync,
        outputs=[json_input, source_radio, property_dropdown, site_dropdown, start_date, end_date, property_dropdown, site_dropdown],
    )
    
    # 実行
    execute_btn.click(
        execute_query,
        inputs=json_input,
        outputs=[result_table, result_chart, status_text],
    )
    
    # CSV保存
    def save_and_show():
        filepath, msg = save_to_csv()
        if filepath:
            return gr.update(value=filepath, visible=True), msg
        return gr.update(visible=False), msg
    
    save_csv_btn.click(
        save_and_show,
        outputs=[csv_file, status_text],
    )


if __name__ == "__main__":
    app.launch(server_name="0.0.0.0", server_port=7860, theme=gr.themes.Soft())
