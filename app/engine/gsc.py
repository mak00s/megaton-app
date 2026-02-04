"""GSC (Search Console) 実行エンジン"""
import pandas as pd
from megaton import start
from ..schemas import QueryParams

# 認証情報パス
CREDS_PATH = "credentials/sa-shibuya-kyousei.json"

# グローバルなmegatonインスタンス（再利用）
_mg = None


def get_megaton():
    """megatonインスタンスを取得（シングルトン）"""
    global _mg
    if _mg is None:
        _mg = start.Megaton(CREDS_PATH, headless=True)
    return _mg


def execute_gsc_query(params: QueryParams) -> pd.DataFrame:
    """GSC クエリを実行してDataFrameを返す"""
    mg = get_megaton()
    
    # サイト選択
    if params.site_url:
        mg.search.use(params.site_url)
    
    # 期間設定
    mg.search.set.dates(params.date_range.start, params.date_range.end)
    
    # レポート実行
    mg.search.run(
        dimensions=params.dimensions,
        metrics=params.metrics,
        limit=params.limit,
    )
    
    df = mg.search.data
    
    # フィルタ適用（後処理）
    for f in params.filters:
        if f.op == "==":
            df = df[df[f.field] == f.value]
        elif f.op == "!=":
            df = df[df[f.field] != f.value]
        elif f.op == "contains":
            df = df[df[f.field].str.contains(f.value, na=False)]
        elif f.op == "not_contains":
            df = df[~df[f.field].str.contains(f.value, na=False)]
    
    return df


def list_gsc_sites() -> list[str]:
    """GSC サイト一覧を取得"""
    mg = get_megaton()
    return mg.search.get.sites()
