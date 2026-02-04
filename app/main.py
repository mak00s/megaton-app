"""Gradio UI メインアプリ"""
import gradio as gr
import pandas as pd
import json
import sys
import os

# パス追加（app/ からの相対インポート用）
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.schemas import QueryParams, SAMPLE_GA4_JSON, SAMPLE_GSC_JSON

# 遅延インポート用（重いモジュールの初期化を実行時まで遅延）
_ga4_module = None
_gsc_module = None
_viz_module = None

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
            return None, None, "データが取得できませんでした"
        
        viz = _get_viz_module()
        
        # チャート生成
        chart = None
        if params.visualization:
            chart = viz.create_chart(df, params.visualization)
        
        # テーブル用にフォーマット
        df_display = viz.format_dataframe(df)
        
        return df_display, chart, f"✓ {len(df)} 行のデータを取得しました"
        
    except json.JSONDecodeError as e:
        return None, None, f"JSONパースエラー: {e}"
    except Exception as e:
        return None, None, f"エラー: {e}"


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


# Gradio UI
with gr.Blocks(title="GA4/GSC 分析ツール") as app:
    gr.Markdown("# GA4 / Search Console 分析ツール")
    gr.Markdown("AI Agent が生成したJSONパラメータを貼り付けて実行します。")
    
    with gr.Row():
        with gr.Column(scale=1):
            gr.Markdown("### パラメータ入力")
            
            json_input = gr.Code(
                label="JSONパラメータ",
                language="json",
                lines=20,
                value=SAMPLE_GA4_JSON,
            )
            
            with gr.Row():
                ga4_sample_btn = gr.Button("GA4 サンプル", size="sm")
                gsc_sample_btn = gr.Button("GSC サンプル", size="sm")
            
            execute_btn = gr.Button("実行", variant="primary", size="lg")
            
            status_text = gr.Textbox(label="ステータス", interactive=False)
            
            with gr.Accordion("プロパティ/サイト一覧", open=False):
                with gr.Row():
                    props_btn = gr.Button("GA4 プロパティ取得", size="sm")
                    sites_btn = gr.Button("GSC サイト取得", size="sm")
                list_output = gr.Textbox(label="一覧", lines=10, interactive=False)
        
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
    
    # イベントハンドラ
    ga4_sample_btn.click(load_ga4_sample, outputs=json_input)
    gsc_sample_btn.click(load_gsc_sample, outputs=json_input)
    
    execute_btn.click(
        execute_query,
        inputs=json_input,
        outputs=[result_table, result_chart, status_text],
    )
    
    props_btn.click(get_properties_list, outputs=list_output)
    sites_btn.click(get_sites_list, outputs=list_output)


if __name__ == "__main__":
    app.launch(server_name="0.0.0.0", server_port=7860, theme=gr.themes.Soft())
