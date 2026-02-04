"""GA4 実行エンジン"""
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


def execute_ga4_query(params: QueryParams) -> pd.DataFrame:
    """GA4 クエリを実行してDataFrameを返す"""
    mg = get_megaton()
    
    # プロパティ選択
    if params.property_id:
        # アカウントIDは自動で解決
        accounts = mg.ga["4"].account.list()
        for acc in accounts:
            acc_id = acc["name"].split("/")[-1]
            props = mg.ga["4"].property.list(acc_id)
            for prop in props:
                prop_id = prop["name"].split("/")[-1]
                if prop_id == params.property_id:
                    mg.ga["4"].account.select(acc_id)
                    mg.ga["4"].property.select(prop_id)
                    break
    
    # 期間設定
    mg.report.set.dates(params.date_range.start, params.date_range.end)
    
    # フィルタ変換
    filters = []
    for f in params.filters:
        filters.append((f.field, f.op, f.value))
    
    # レポート実行
    mg.report.run(
        d=params.dimensions,
        m=params.metrics,
        filters=filters if filters else None,
        limit=params.limit,
        show=False,
    )
    
    return mg.report.data


def list_ga4_properties() -> list[dict]:
    """GA4 プロパティ一覧を取得"""
    mg = get_megaton()
    result = []
    
    accounts = mg.ga["4"].account.list()
    for acc in accounts:
        acc_id = acc["name"].split("/")[-1]
        acc_name = acc["displayName"]
        props = mg.ga["4"].property.list(acc_id)
        for prop in props:
            prop_id = prop["name"].split("/")[-1]
            prop_name = prop["displayName"]
            result.append({
                "account_id": acc_id,
                "account_name": acc_name,
                "property_id": prop_id,
                "property_name": prop_name,
                "display": f"{prop_name} ({prop_id})"
            })
    
    return result
