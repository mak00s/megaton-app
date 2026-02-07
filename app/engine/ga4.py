"""GA4 実行エンジン"""
import pandas as pd
from megaton import start
from ..schemas import QueryParams
from megaton_lib.credentials import resolve_service_account_path

# デフォルトGA4設定（フォールバック用）
DEFAULT_GA4_ACCOUNT = "000000000"
DEFAULT_GA4_PROPERTY = "000000000"

# シングルトン
_mg = None

def get_megaton():
    """megatonインスタンスを取得"""
    global _mg
    if _mg is None:
        _mg = start.Megaton(resolve_service_account_path(), headless=True)
    return _mg


def execute_ga4_query(params: QueryParams) -> pd.DataFrame:
    """GA4 クエリを実行してDataFrameを返す"""
    mg = get_megaton()
    
    # プロパティIDからアカウントIDを自動解決
    property_id = params.property_id or DEFAULT_GA4_PROPERTY
    account_id = None
    
    for acc in mg.ga["4"].accounts:
        for prop in acc.get("properties", []):
            if prop["id"] == property_id:
                account_id = acc["id"]
                break
        if account_id:
            break
    
    if not account_id:
        account_id = DEFAULT_GA4_ACCOUNT
    
    mg.ga["4"].account.select(account_id)
    mg.ga["4"].property.select(property_id)
    
    # 期間設定
    mg.report.set.dates(params.date_range.start, params.date_range.end)
    
    # フィルタを文字列に変換（複数はセミコロン区切り）
    filter_d = None
    if params.filters:
        filter_parts = [f"{f.field}{f.op}{f.value}" for f in params.filters]
        filter_d = ";".join(filter_parts)
    
    # レポート実行
    mg.report.run(
        d=params.dimensions,
        m=params.metrics,
        filter_d=filter_d,
        limit=params.limit,
        show=False,
    )
    
    return mg.report.data


def list_ga4_properties() -> list[dict]:
    """GA4 プロパティ一覧を取得"""
    mg = get_megaton()
    result = []
    
    for acc in mg.ga["4"].accounts:
        acc_id = acc["id"]
        acc_name = acc["name"]
        for prop in acc.get("properties", []):
            result.append({
                "account_id": acc_id,
                "account_name": acc_name,
                "property_id": prop["id"],
                "property_name": prop["name"],
                "display": f"{prop['name']} ({prop['id']})"
            })
    
    return result
