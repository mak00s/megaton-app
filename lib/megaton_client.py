"""megaton共通クライアント - Streamlit/CLI両方から使用

複数のサービスアカウントJSONを自動検出し、
property_id / site_url に応じて正しいクレデンシャルに自動ルーティングする。
"""
from megaton import start
import pandas as pd
from typing import Optional
from lib.credentials import list_service_account_paths

# === レジストリ（複数クレデンシャル管理） ===

_instances: dict[str, object] = {}     # creds_path → Megaton instance
_property_map: dict[str, str] = {}     # property_id → creds_path
_site_map: dict[str, str] = {}         # site_url → creds_path
_registry_built = False


def get_megaton(creds_path: str | None = None):
    """Megatonインスタンスを取得（creds_path別にシングルトン）

    creds_path=None の場合、最初に見つかったクレデンシャルを使用。
    """
    if creds_path is None:
        paths = list_service_account_paths()
        if not paths:
            raise FileNotFoundError(
                "No service account JSON found. "
                "Place a JSON file in credentials/ or set MEGATON_CREDS_PATH."
            )
        creds_path = paths[0]
    if creds_path not in _instances:
        _instances[creds_path] = start.Megaton(creds_path, headless=True)
    return _instances[creds_path]


def build_registry() -> None:
    """全クレデンシャルからGA4プロパティ・GSCサイトのマッピングを構築（初回のみ）"""
    global _registry_built
    if _registry_built:
        return

    paths = list_service_account_paths()

    for path in paths:
        mg = get_megaton(path)
        # GA4
        try:
            for acc in mg.ga["4"].accounts:
                for prop in acc.get("properties", []):
                    _property_map[prop["id"]] = path
        except Exception:
            pass  # このクレデンシャルにGA4アクセスがない場合はスキップ
        # GSC
        try:
            sites = mg.search.get.sites()
            for site in sites:
                _site_map[site] = path
        except Exception:
            pass  # このクレデンシャルにGSCアクセスがない場合はスキップ

    _registry_built = True


def get_megaton_for_property(property_id: str):
    """指定GA4プロパティに対応するMegatonインスタンスを返す"""
    build_registry()
    creds_path = _property_map.get(property_id)
    if creds_path is None:
        raise ValueError(f"No credential found for property_id: {property_id}")
    return get_megaton(creds_path)


def get_megaton_for_site(site_url: str):
    """指定GSCサイトに対応するMegatonインスタンスを返す"""
    build_registry()
    creds_path = _site_map.get(site_url)
    if creds_path is None:
        raise ValueError(f"No credential found for site_url: {site_url}")
    return get_megaton(creds_path)


# === GA4 ===

def get_ga4_properties() -> list:
    """全クレデンシャルのGA4プロパティを統合して返す"""
    build_registry()
    result = []
    seen_ids: set[str] = set()
    for path in dict.fromkeys(_property_map.values()):
        mg = get_megaton(path)
        for acc in mg.ga["4"].accounts:
            for prop in acc.get("properties", []):
                if prop["id"] not in seen_ids:
                    seen_ids.add(prop["id"])
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
    """GA4クエリを実行（自動ルーティング）"""
    mg = get_megaton_for_property(property_id)

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
    """全クレデンシャルのGSCサイトを統合して返す"""
    build_registry()
    seen: set[str] = set()
    result = []
    for path in dict.fromkeys(_site_map.values()):
        mg = get_megaton(path)
        for site in mg.search.get.sites():
            if site not in seen:
                seen.add(site)
                result.append(site)
    return result


def query_gsc(
    site_url: str,
    start_date: str,
    end_date: str,
    dimensions: list,
    limit: int = 25000,
    dimension_filter: Optional[list] = None
) -> pd.DataFrame:
    """GSCクエリを実行（自動ルーティング）

    Args:
        dimension_filter: フィルタ条件のリスト
            例: [{"dimension": "query", "operator": "contains", "expression": "渋谷"}]
            演算子: contains, notContains, equals, notEquals, includingRegex, excludingRegex
    """
    mg = get_megaton_for_site(site_url)
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


def save_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    df: pd.DataFrame,
    mode: str = "overwrite",
) -> dict:
    """BigQueryテーブルに保存

    Args:
        project_id: GCPプロジェクトID
        dataset_id: データセットID
        table_id: テーブルID
        df: 保存するDataFrame
        mode: "overwrite" or "append"

    Returns:
        dict with table reference and row count
    """
    from google.cloud import bigquery as bq_lib

    bq = get_bigquery(project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    disposition_map = {
        "overwrite": bq_lib.WriteDisposition.WRITE_TRUNCATE,
        "append": bq_lib.WriteDisposition.WRITE_APPEND,
    }
    if mode not in disposition_map:
        raise ValueError(f"Unsupported mode for BigQuery: {mode}")

    job_config = bq_lib.LoadJobConfig(
        write_disposition=disposition_map[mode],
        autodetect=True,
    )

    job = bq.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # 完了まで待つ

    table = bq.client.get_table(table_ref)
    return {
        "table": table_ref,
        "row_count": table.num_rows,
    }


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
