"""megaton共通クライアント - Streamlit/CLI両方から使用"""
from megaton import start
import pandas as pd
from typing import Optional
from functools import lru_cache

CREDS_PATH = "credentials/sa-shibuya-kyousei.json"

# シングルトンインスタンス
_mg = None

def get_megaton():
    """megatonインスタンスを取得（シングルトン）"""
    global _mg
    if _mg is None:
        _mg = start.Megaton(CREDS_PATH, headless=True)
    return _mg


# === GA4 ===

def get_ga4_properties() -> list:
    """GA4プロパティ一覧を取得"""
    mg = get_megaton()
    result = []
    for acc in mg.ga["4"].accounts:
        for prop in acc.get("properties", []):
            result.append({
                "id": prop["id"],
                "name": prop["name"],
                "account_id": acc["id"],
                "display": f"{prop['name']} ({prop['id']})"
            })
    return result


def query_ga4(
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: list,
    metrics: list,
    filter_d: Optional[str] = None,
    limit: int = 10000
) -> pd.DataFrame:
    """GA4クエリを実行"""
    mg = get_megaton()
    
    # プロパティに紐づくアカウントを探して選択
    for acc in mg.ga["4"].accounts:
        for prop in acc.get("properties", []):
            if prop["id"] == property_id:
                mg.ga["4"].account.select(acc["id"])
                mg.ga["4"].property.select(property_id)
                break
    
    mg.report.set.dates(start_date, end_date)
    mg.report.run(
        d=dimensions,
        m=metrics,
        filter_d=filter_d if filter_d else None,
        limit=limit,
        show=False
    )
    return mg.report.data


# === GSC ===

def get_gsc_sites() -> list:
    """GSCサイト一覧を取得"""
    mg = get_megaton()
    return mg.search.get.sites()


def query_gsc(
    site_url: str,
    start_date: str,
    end_date: str,
    dimensions: list,
    limit: int = 25000,
    dimension_filter: Optional[list] = None
) -> pd.DataFrame:
    """GSCクエリを実行
    
    Args:
        dimension_filter: フィルタ条件のリスト
            例: [{"dimension": "query", "operator": "contains", "expression": "渋谷"}]
            演算子: contains, notContains, equals, notEquals, includingRegex, excludingRegex
    """
    mg = get_megaton()
    mg.search.use(site_url)
    mg.search.set.dates(start_date, end_date)
    mg.search.run(
        dimensions=dimensions,
        metrics=["clicks", "impressions", "ctr", "position"],
        limit=limit,
        dimension_filter=dimension_filter
    )
    return mg.search.data


# === BigQuery ===

_bq_clients = {}

def get_bigquery(project_id: str):
    """BigQueryクライアントを取得"""
    if project_id not in _bq_clients:
        mg = get_megaton()
        _bq_clients[project_id] = mg.launch_bigquery(project_id)
    return _bq_clients[project_id]


def get_bq_datasets(project_id: str) -> list:
    """BigQueryデータセット一覧を取得"""
    bq = get_bigquery(project_id)
    return bq.datasets


def query_bq(project_id: str, sql: str) -> pd.DataFrame:
    """BigQueryクエリを実行"""
    bq = get_bigquery(project_id)
    return bq.run(sql, to_dataframe=True)


# === Google Sheets ===

def save_to_sheet(
    sheet_url: str,
    sheet_name: str,
    df: pd.DataFrame,
    mode: str = "overwrite",
    keys: Optional[list] = None
):
    """Google Sheetsに保存
    
    Args:
        sheet_url: スプレッドシートURL
        sheet_name: シート名
        df: 保存するDataFrame
        mode: "overwrite", "append", "upsert"
        keys: アップサート時のキー列
    """
    mg = get_megaton()
    mg.open.sheet(sheet_url)
    
    if mode == "overwrite":
        mg.save.to.sheet(sheet_name, df, freeze_header=True)
    elif mode == "append":
        mg.append.to.sheet(sheet_name, df)
    elif mode == "upsert":
        if not keys:
            raise ValueError("upsertモードではkeys引数が必要です")
        mg.upsert.to.sheet(sheet_name, df, keys=keys)
