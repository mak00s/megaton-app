"""可視化モジュール"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ..schemas import Visualization


def create_chart(df: pd.DataFrame, viz: Visualization):
    """DataFrameと可視化設定からPlotlyチャートを生成"""
    if viz is None or viz.type == "table":
        return None
    
    title = viz.title or ""
    
    if viz.type == "line":
        fig = px.line(
            df,
            x=viz.x,
            y=viz.y,
            title=title,
            markers=True,
        )
    elif viz.type == "bar":
        # 上位N件に制限
        df_plot = df.head(20) if len(df) > 20 else df
        fig = px.bar(
            df_plot,
            x=viz.x,
            y=viz.y,
            title=title,
        )
    elif viz.type == "pie":
        df_plot = df.head(10) if len(df) > 10 else df
        fig = px.pie(
            df_plot,
            names=viz.x,
            values=viz.y,
            title=title,
        )
    else:
        return None
    
    # レイアウト調整
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Noto Sans JP, sans-serif"),
    )
    
    return fig


def format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrameを表示用にフォーマット"""
    df = df.copy()
    
    # 数値フォーマット
    for col in df.columns:
        if df[col].dtype in ['int64', 'int32']:
            df[col] = df[col].apply(lambda x: f"{x:,}")
        elif df[col].dtype in ['float64', 'float32']:
            if col in ['ctr']:
                df[col] = df[col].apply(lambda x: f"{x:.2%}")
            elif col in ['position']:
                df[col] = df[col].apply(lambda x: f"{x:.1f}")
            else:
                df[col] = df[col].apply(lambda x: f"{x:,.2f}")
    
    return df
