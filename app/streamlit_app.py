"""Streamlit UI main app"""
import sys
from pathlib import Path

# Ensure project root is on sys.path so `app.*` / `megaton_lib.*` resolve
# when invoked as `streamlit run app/streamlit_app.py`.
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import json
import os
import re
from datetime import datetime, timedelta

from app.i18n import (
    t,
    t_option_map,
    t_options,
    init_language,
    language_selector,
    translated_select_model,
)

# Language must be initialised before set_page_config
init_language()

st.set_page_config(
    page_title=t("page.title"),
    page_icon="📊",
    layout="wide",
)

# === Parameter file watching ===

PARAMS_FILE = Path("input/params.json")

def load_params_from_file():
    """Load parameters from external JSON file"""
    if not PARAMS_FILE.exists():
        return None, None, [], None
    try:
        mtime = PARAMS_FILE.stat().st_mtime
        with open(PARAMS_FILE, "r", encoding="utf-8") as f:
            raw_params = json.load(f)
        canonical = canonicalize_json(raw_params)
        params, errors = validate_params(raw_params)
        return params, mtime, errors, canonical
    except json.JSONDecodeError as e:
        return None, None, [{
            "error_code": "INVALID_JSON",
            "message": f"Invalid JSON: {e}",
            "path": "$",
            "hint": "Fix JSON syntax in input/params.json."
        }], None
    except IOError as e:
        return None, None, [{
            "error_code": "FILE_IO_ERROR",
            "message": f"Failed to read params.json: {e}",
            "path": "$",
            "hint": "Check file permissions and file path."
        }], None

def apply_params_to_session(params):
    """Apply loaded parameters to session state (including widget keys)"""
    if params is None:
        return False

    st.session_state["loaded_params"] = params
    st.session_state["params_applied"] = True

    source = params.get("source", "ga4").lower()

    # Dates
    date_range = params.get("date_range", {})
    if date_range.get("start"):
        st.session_state["w_start_date"] = datetime.strptime(date_range["start"], "%Y-%m-%d").date()
    if date_range.get("end"):
        st.session_state["w_end_date"] = datetime.strptime(date_range["end"], "%Y-%m-%d").date()

    # Row limit
    if "limit" in params:
        st.session_state["w_limit"] = params["limit"]

    # Dimensions
    if "dimensions" in params:
        if source == "gsc":
            st.session_state["w_gsc_dimensions"] = params["dimensions"]
        elif source == "ga4":
            st.session_state["w_ga4_dimensions"] = params["dimensions"]

    # GA4 specific
    if source == "ga4":
        if "property_id" in params:
            st.session_state["w_ga4_property_id"] = params["property_id"]
        if "metrics" in params:
            st.session_state["w_ga4_metrics"] = params["metrics"]
        st.session_state["w_ga4_filter"] = params.get("filter_d", "")

    # GSC specific
    if source == "gsc":
        if "site_url" in params:
            st.session_state["w_gsc_site"] = params["site_url"]
        st.session_state["w_gsc_filter"] = params.get("filter", "")

    # AA specific
    if source == "aa":
        if "company_id" in params:
            st.session_state["w_aa_company_id"] = params["company_id"]
        if "org_id" in params:
            st.session_state["w_aa_org_id"] = params["org_id"]
        if "rsid" in params:
            st.session_state["w_aa_rsid"] = params["rsid"]
        if "dimension" in params:
            st.session_state["w_aa_dimension"] = params["dimension"]
        if "metrics" in params:
            st.session_state["w_aa_metrics"] = params["metrics"]
        segment_val = params.get("segment", "")
        if isinstance(segment_val, list):
            st.session_state["w_aa_segment"] = ",".join(str(x).strip() for x in segment_val if str(x).strip())
        elif isinstance(segment_val, str):
            st.session_state["w_aa_segment"] = segment_val

    # BigQuery specific
    if source == "bigquery":
        if "project_id" in params:
            st.session_state["w_bq_project"] = params["project_id"]
        if "sql" in params:
            st.session_state["w_bq_sql"] = params["sql"]

    # Pipeline (reset all before applying)
    pipeline = params.get("pipeline") or {}
    st.session_state["w_tf_date"] = False
    st.session_state["w_tf_url_decode"] = False
    st.session_state["w_tf_strip_qs"] = False
    st.session_state["w_tf_keep_params"] = ""
    st.session_state["w_tf_path_only"] = False
    st.session_state["w_pipeline_where"] = ""
    st.session_state["w_pipeline_columns"] = []
    st.session_state["w_pipeline_group_by"] = []
    st.session_state["w_pipeline_head"] = 0
    for key in list(st.session_state.keys()):
        if key.startswith("w_agg_"):
            del st.session_state[key]

    if pipeline:
        if pipeline.get("transform"):
            expr = pipeline["transform"]
            try:
                transforms = parse_transforms(expr)
            except ValueError:
                transforms = []
            for _, func, args in transforms:
                if func == "date_format":
                    st.session_state["w_tf_date"] = True
                elif func == "url_decode":
                    st.session_state["w_tf_url_decode"] = True
                elif func == "strip_qs":
                    st.session_state["w_tf_strip_qs"] = True
                    if args:
                        st.session_state["w_tf_keep_params"] = args
                elif func == "path_only":
                    st.session_state["w_tf_path_only"] = True
        if pipeline.get("where"):
            st.session_state["w_pipeline_where"] = pipeline["where"]
        if pipeline.get("columns"):
            st.session_state["w_pipeline_columns"] = [
                c.strip() for c in pipeline["columns"].split(",") if c.strip()
            ]
        if pipeline.get("group_by"):
            st.session_state["w_pipeline_group_by"] = [
                c.strip() for c in pipeline["group_by"].split(",") if c.strip()
            ]
        if pipeline.get("aggregate"):
            for part in pipeline["aggregate"].split(","):
                tokens = [x.strip() for x in part.split(":", 1)]
                if len(tokens) == 2 and tokens[0] and tokens[1]:
                    func, col = tokens
                    st.session_state[f"w_agg_{col}"] = func
        if pipeline.get("head") is not None:
            st.session_state["w_pipeline_head"] = pipeline["head"]

    # save
    save = params.get("save") or {}
    st.session_state["w_sheet_url"] = ""
    st.session_state["w_sheet_name"] = "data"
    st.session_state["w_save_mode"] = "overwrite"
    st.session_state["w_save_bq_project"] = ""
    st.session_state["w_save_bq_dataset"] = ""
    st.session_state["w_save_bq_table"] = ""
    st.session_state["w_save_bq_mode"] = "overwrite"
    st.session_state["w_save_filename"] = ""
    # Clear selectbox widget keys so they pick up the new internal value
    st.session_state.pop("w_save_mode_select", None)
    st.session_state.pop("w_save_bq_mode_select", None)

    if save:
        target = save.get("to")

        if target == "csv":
            path = save.get("path", "")
            if path:
                st.session_state["w_save_filename"] = Path(path).name

        elif target == "sheets":
            st.session_state["w_sheet_url"] = save.get("sheet_url", "")
            st.session_state["w_sheet_name"] = save.get("sheet_name", "data")
            st.session_state["w_save_mode"] = save.get("mode", "overwrite")
            if save.get("keys"):
                st.session_state["w_upsert_keys"] = save["keys"]

        elif target == "bigquery":
            st.session_state["w_save_bq_project"] = save.get("project_id", "")
            st.session_state["w_save_bq_dataset"] = save.get("dataset", "")
            st.session_state["w_save_bq_table"] = save.get("table", "")
            st.session_state["w_save_bq_mode"] = save.get("mode", "overwrite")

    return True

def check_file_updated():
    """Check for file updates (mtime + content diff)"""
    if not PARAMS_FILE.exists():
        return False, None, None, []

    current_mtime = PARAMS_FILE.stat().st_mtime
    last_mtime = st.session_state.get("last_params_mtime", 0)

    if current_mtime <= last_mtime:
        return False, None, None, []

    params, mtime, errors, canonical = load_params_from_file()
    st.session_state["last_params_mtime"] = current_mtime

    last_canonical = st.session_state.get("last_params_canonical")
    if not has_effective_params_update(current_mtime, last_mtime, canonical, last_canonical):
        return False, None, None, []

    st.session_state["last_params_canonical"] = canonical
    return True, params, mtime, errors

# === Imports ===
import megaton_lib.megaton_client as _megaton_client

get_megaton = _megaton_client.get_megaton
_get_ga4_properties = _megaton_client.get_ga4_properties
query_ga4 = _megaton_client.query_ga4
_get_gsc_sites = _megaton_client.get_gsc_sites
query_gsc = _megaton_client.query_gsc
query_aa = getattr(_megaton_client, "query_aa", None)
_get_bq_datasets = _megaton_client.get_bq_datasets
query_bq = _megaton_client.query_bq
save_to_sheet = _megaton_client.save_to_sheet
save_to_bq = _megaton_client.save_to_bq
from megaton_lib.params_diff import canonicalize_json
from megaton_lib.params_validator import validate_params
from megaton_lib.result_inspector import apply_pipeline, SUPPORTED_AGG_FUNCS, parse_transforms
from app.ui.params_utils import (
    GA4_OPERATORS,
    GSC_OPERATORS,
    COL_FIELD,
    COL_OPERATOR,
    COL_VALUE,
    parse_ga4_filter_to_df,
    serialize_ga4_filter_from_df,
    parse_gsc_filter_to_df,
    serialize_gsc_filter_from_df,
    has_effective_params_update,
)
from app.ui.query_builders import (
    AGG_NONE,
    parse_gsc_filter,
    detect_url_columns,
    build_transform_expression,
    build_pipeline_kwargs,
    build_agent_params,
)
from app.ui.ga4_fields import ALL_DIMENSIONS, ALL_METRICS

# Streamlit cached wrappers
@st.cache_data(ttl=300)
def get_ga4_properties():
    return _get_ga4_properties()

@st.cache_data(ttl=300)
def get_gsc_sites():
    return _get_gsc_sites()

@st.cache_data(ttl=300)
def get_bq_datasets(project_id):
    return _get_bq_datasets(project_id)

@st.cache_data(ttl=60)
def execute_ga4_query(property_id, start_date, end_date, dimensions, metrics, filter_d, limit):
    return query_ga4(property_id, start_date, end_date, dimensions, metrics, filter_d, limit)

@st.cache_data(ttl=60)
def execute_gsc_query(site_url, start_date, end_date, dimensions, limit, dimension_filter=None):
    return query_gsc(site_url, start_date, end_date, dimensions, limit, dimension_filter)

@st.cache_data(ttl=60)
def execute_aa_query(company_id, rsid, start_date, end_date, dimension, metrics, segment, limit, org_id=""):
    if query_aa is None:
        raise RuntimeError(
            "AA query function is not available in megaton_lib.megaton_client. "
            "Please restart Streamlit and ensure the latest code is loaded."
        )
    return query_aa(
        company_id=company_id,
        rsid=rsid,
        start_date=start_date,
        end_date=end_date,
        dimension=dimension,
        metrics=metrics,
        segment=segment,
        limit=limit,
        org_id=org_id or None,
    )

def execute_bq_query(project_id, sql):
    return query_bq(project_id, sql)


def detect_datetime_x_axis(df: pd.DataFrame) -> tuple[str | None, pd.Series | None]:
    """Pick a datetime-like column for chart X-axis when available."""
    if df.empty:
        return None, None

    if "date" in df.columns:
        parsed = pd.to_datetime(df["date"], errors="coerce")
        if parsed.notna().any():
            return "date", parsed

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().any():
                return col, parsed

    time_name_hints = ("date", "day", "time", "month", "week")
    hinted = [c for c in df.columns if any(h in str(c).lower() for h in time_name_hints)]
    others = [c for c in df.columns if c not in hinted]

    for col in [*hinted, *others]:
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            continue
        parsed = pd.to_datetime(series, errors="coerce")
        if len(parsed) == 0:
            continue
        if float(parsed.notna().mean()) >= 0.8:
            return col, parsed

    return None, None


# === UI ===

st.title(t("page.heading"))

# === File watching section ===

# Session initialisation
if "auto_watch" not in st.session_state:
    st.session_state["auto_watch"] = True
if "auto_execute" not in st.session_state:
    st.session_state["auto_execute"] = False
if "params_validation_errors" not in st.session_state:
    st.session_state["params_validation_errors"] = []
if "last_params_canonical" not in st.session_state:
    st.session_state["last_params_canonical"] = None
if "execute_requested" not in st.session_state:
    st.session_state["execute_requested"] = False
if "is_executing" not in st.session_state:
    st.session_state["is_executing"] = False

# Auto-refresh (file watching: every 2s)
if st.session_state.get("auto_watch", True):
    st_autorefresh(interval=2000, limit=None, key="file_watcher_refresh")

# File change check flag
file_just_updated = False

# File change check
if st.session_state.get("auto_watch", True):
    updated, params, _, errors = check_file_updated()
    if updated:
        if params:
            apply_params_to_session(params)
            st.session_state["params_validation_errors"] = []
            st.toast(t("msg.params_file_updated"), icon="📄")
            file_just_updated = True
            if st.session_state.get("auto_execute", False):
                st.session_state["auto_execute_pending"] = True
        elif errors:
            st.session_state["params_validation_errors"] = errors
            st.toast(t("msg.params_validation_failed"), icon="⚠️")

# Save mode option maps (internal values)
SAVE_MODE_KEYS = {
    "save.mode_overwrite": "overwrite",
    "save.mode_append": "append",
    "save.mode_upsert": "upsert",
}
SAVE_BQ_MODE_KEYS = {
    "save.mode_overwrite": "overwrite",
    "save.mode_append": "append",
}

# Sidebar
with st.sidebar:
    st.header(t("sidebar.settings"))

    validation_errors = st.session_state.get("params_validation_errors", [])
    if validation_errors:
        st.error(t("msg.params_schema_mismatch"))
        for err in validation_errors:
            st.caption(f"`{err['error_code']}` {err['path']} - {err['message']}")

    # Loaded params
    lp = st.session_state.get("loaded_params", {})

    # Notification after params applied
    if st.session_state.get("params_applied"):
        st.info(t("msg.params_applied"))
        st.session_state["params_applied"] = False

    # Data source selection
    source_map = {"ga4": "GA4", "gsc": "GSC", "aa": "AA", "bigquery": "BigQuery"}
    default_source = source_map.get(lp.get("source", "ga4").lower(), "GA4")
    source_options = ["GA4", "GSC", "AA", "BigQuery"]
    source = st.radio(
        t("sidebar.source"),
        source_options,
        horizontal=True,
        index=source_options.index(default_source),
    )

    st.divider()

    # Date range (not for BigQuery)
    if source != "BigQuery":
        if "w_start_date" not in st.session_state:
            date_range = lp.get("date_range", {})
            st.session_state["w_start_date"] = datetime.strptime(date_range["start"], "%Y-%m-%d").date() if date_range.get("start") else (datetime.now() - timedelta(days=14)).date()
            st.session_state["w_end_date"] = datetime.strptime(date_range["end"], "%Y-%m-%d").date() if date_range.get("end") else (datetime.now() - timedelta(days=1)).date()

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(t("sidebar.start_date"), key="w_start_date")
        with col2:
            end_date = st.date_input(t("sidebar.end_date"), key="w_end_date")

        st.divider()

    if source == "GA4":
        # GA4 settings
        try:
            properties = get_ga4_properties()
        except (RuntimeError, FileNotFoundError, ValueError) as e:
            st.error(t("msg.auth_error", error=str(e)))
            st.stop()
        property_options = {p["display"]: p["id"] for p in properties}

        # Reverse lookup property ID to display name
        default_prop_idx = 0
        loaded_prop_id = st.session_state.get("w_ga4_property_id") or lp.get("property_id", "")
        for i, (display, pid) in enumerate(property_options.items()):
            if pid == loaded_prop_id:
                default_prop_idx = i
                break

        selected_property = st.selectbox(t("ga4.property"), list(property_options.keys()), index=default_prop_idx)
        property_id = property_options[selected_property]

        # Dimensions
        all_dimensions = ALL_DIMENSIONS
        if "w_ga4_dimensions" not in st.session_state:
            st.session_state["w_ga4_dimensions"] = lp.get("dimensions", ["date"]) if lp.get("source", "").lower() == "ga4" else ["date"]
        dimensions = st.multiselect(t("ga4.dimensions"), all_dimensions, key="w_ga4_dimensions",
                                    accept_new_options=True, max_selections=9)

        # Metrics
        all_metrics = ALL_METRICS
        if "w_ga4_metrics" not in st.session_state:
            st.session_state["w_ga4_metrics"] = lp.get("metrics", ["sessions", "activeUsers"]) if lp.get("source", "").lower() == "ga4" else ["sessions", "activeUsers"]
        metrics = st.multiselect(t("ga4.metrics"), all_metrics, key="w_ga4_metrics",
                                 accept_new_options=True, max_selections=10)

        # Filter
        if "w_ga4_filter" not in st.session_state:
            st.session_state["w_ga4_filter"] = lp.get("filter_d", "") if lp.get("source", "").lower() == "ga4" else ""

        filter_df = parse_ga4_filter_to_df(st.session_state.get("w_ga4_filter", ""))

        with st.expander(t("ga4.filter"), expanded=bool(len(filter_df))):
            edited_filter_df = st.data_editor(
                filter_df,
                column_config={
                    COL_FIELD: st.column_config.SelectboxColumn(
                        t("filter.field"),
                        options=list(dict.fromkeys(all_dimensions + dimensions)),
                        required=True,
                    ),
                    COL_OPERATOR: st.column_config.SelectboxColumn(
                        t("filter.operator"),
                        options=GA4_OPERATORS,
                        required=True,
                    ),
                    COL_VALUE: st.column_config.TextColumn(t("filter.value"), required=True),
                },
                num_rows="dynamic",
                width="stretch",
                key="ga4_filter_editor"
            )

            filter_d = serialize_ga4_filter_from_df(edited_filter_df)
            st.session_state["w_ga4_filter"] = filter_d

            if filter_d:
                st.caption(f"📝 `{filter_d}`")

    elif source == "GSC":
        # GSC settings
        try:
            sites = get_gsc_sites()
        except (RuntimeError, FileNotFoundError, ValueError) as e:
            st.error(t("msg.auth_error", error=str(e)))
            st.stop()

        if "w_gsc_site" not in st.session_state:
            loaded_site_url = lp.get("site_url", "")
            if loaded_site_url in sites:
                st.session_state["w_gsc_site"] = loaded_site_url
            elif sites:
                st.session_state["w_gsc_site"] = sites[0]

        site_url = st.selectbox(t("gsc.site"), sites, key="w_gsc_site")

        # Dimensions
        all_gsc_dims = ["query", "page", "country", "device", "date"]
        if "w_gsc_dimensions" not in st.session_state:
            st.session_state["w_gsc_dimensions"] = lp.get("dimensions", ["query"]) if lp.get("source", "").lower() == "gsc" else ["query"]
        dimensions = st.multiselect(t("gsc.dimensions"), all_gsc_dims, key="w_gsc_dimensions")

        # Filter
        if "w_gsc_filter" not in st.session_state:
            st.session_state["w_gsc_filter"] = lp.get("filter", "") if lp.get("source", "").lower() == "gsc" else ""

        gsc_filter_df = parse_gsc_filter_to_df(st.session_state.get("w_gsc_filter", ""))

        with st.expander(t("gsc.filter"), expanded=bool(len(gsc_filter_df))):
            gsc_filter_dims = ["query", "page", "country", "device", "date"]

            edited_gsc_filter_df = st.data_editor(
                gsc_filter_df,
                column_config={
                    COL_FIELD: st.column_config.SelectboxColumn(
                        t("filter.field"),
                        options=gsc_filter_dims,
                        required=True,
                    ),
                    COL_OPERATOR: st.column_config.SelectboxColumn(
                        t("filter.operator"),
                        options=GSC_OPERATORS,
                        required=True,
                    ),
                    COL_VALUE: st.column_config.TextColumn(t("filter.value"), required=True),
                },
                num_rows="dynamic",
                width="stretch",
                key="gsc_filter_editor"
            )

            gsc_filter = serialize_gsc_filter_from_df(edited_gsc_filter_df)
            st.session_state["w_gsc_filter"] = gsc_filter

            if gsc_filter:
                st.caption(f"📝 `{gsc_filter}`")

    elif source == "AA":
        if "w_aa_company_id" not in st.session_state:
            st.session_state["w_aa_company_id"] = lp.get("company_id", "") if lp.get("source", "").lower() == "aa" else ""
        if "w_aa_org_id" not in st.session_state:
            st.session_state["w_aa_org_id"] = lp.get("org_id", "") if lp.get("source", "").lower() == "aa" else ""
        if "w_aa_rsid" not in st.session_state:
            st.session_state["w_aa_rsid"] = lp.get("rsid", "") if lp.get("source", "").lower() == "aa" else ""
        if "w_aa_dimension" not in st.session_state:
            st.session_state["w_aa_dimension"] = lp.get("dimension", "daterangeday") if lp.get("source", "").lower() == "aa" else "daterangeday"
        if "w_aa_metrics" not in st.session_state:
            default_aa_metrics = lp.get("metrics", ["occurrences"]) if lp.get("source", "").lower() == "aa" else ["occurrences"]
            st.session_state["w_aa_metrics"] = default_aa_metrics
        if "w_aa_segment" not in st.session_state:
            loaded_segment = lp.get("segment", "") if lp.get("source", "").lower() == "aa" else ""
            if isinstance(loaded_segment, list):
                loaded_segment = ",".join(str(x).strip() for x in loaded_segment if str(x).strip())
            st.session_state["w_aa_segment"] = loaded_segment

        aa_col1, aa_col2 = st.columns(2)
        with aa_col1:
            company_id = st.text_input(t("aa.company_id"), key="w_aa_company_id")
            rsid = st.text_input(t("aa.rsid"), key="w_aa_rsid")
        with aa_col2:
            org_id = st.text_input(t("aa.org_id"), key="w_aa_org_id")
            dimension = st.text_input(t("aa.dimension"), key="w_aa_dimension")

        aa_metric_options = [
            "occurrences",
            "revenue",
            "orders",
            "units",
            "visits",
            "pageviews",
            "visitors",
            "uniquevisitors",
        ]
        metrics = st.multiselect(
            t("aa.metrics"),
            aa_metric_options,
            key="w_aa_metrics",
            accept_new_options=True,
            max_selections=20,
        )
        aa_segment_raw = st.text_input(
            t("aa.segment"),
            key="w_aa_segment",
            placeholder="s12345,s67890",
        )
        aa_segments = [s.strip() for s in re.split(r"[,\\n;]", aa_segment_raw) if s.strip()]

    else:
        # BigQuery settings
        if "w_bq_project" not in st.session_state:
            st.session_state["w_bq_project"] = lp.get("project_id", "")
        bq_project = st.text_input(t("bq.project_id"), key="w_bq_project")

    # Row limit (not for BigQuery)
    if source != "BigQuery":
        if "w_limit" not in st.session_state:
            st.session_state["w_limit"] = lp.get("limit", 1000)
        limit_options = [100, 500, 1000, 5000, 10000, 25000, 50000, 100000]
        limit_labels = {v: f"{v:,}" for v in limit_options}

        current_limit = st.session_state.get("w_limit", 1000)
        if current_limit not in limit_options:
            current_limit = min(limit_options, key=lambda x: abs(x - current_limit))

        limit = st.select_slider(
            t("sidebar.limit"),
            options=limit_options,
            value=current_limit,
            format_func=lambda x: limit_labels[x],
            key="w_limit"
        )

    st.divider()

    execute_btn = st.button(
        t("sidebar.execute_running") if st.session_state.get("is_executing") else t("sidebar.execute"),
        type="primary",
        width="stretch",
        disabled=st.session_state.get("is_executing", False),
    )
    if execute_btn and not st.session_state.get("is_executing", False):
        st.session_state["execute_requested"] = True
        st.session_state["is_executing"] = True
        st.rerun()

    if st.session_state.get("is_executing", False):
        st.caption(t("msg.fetching"))

    st.divider()

    language_selector()

    with st.expander(t("agent.header"), expanded=False):
        st.session_state["auto_watch"] = st.toggle(
            t("agent.auto_watch"),
            value=st.session_state.get("auto_watch", True),
            help=t("agent.auto_watch_help")
        )
        st.session_state["auto_execute"] = st.toggle(
            t("agent.auto_execute"),
            value=st.session_state.get("auto_execute", False),
            help=t("agent.auto_execute_help")
        )

        # File status display
        if PARAMS_FILE.exists():
            mtime = datetime.fromtimestamp(PARAMS_FILE.stat().st_mtime)
            st.caption(t("agent.params_updated", time=mtime.strftime('%H:%M:%S')))
        else:
            st.caption(t("agent.params_none"))

        # Manual load button
        if st.button(t("agent.load_json"), width="stretch"):
            params, mtime, errors, canonical = load_params_from_file()
            if params:
                apply_params_to_session(params)
                st.session_state["last_params_mtime"] = mtime
                st.session_state["last_params_canonical"] = canonical
                st.session_state["params_validation_errors"] = []
                st.success(t("msg.params_loaded"))
                st.rerun()
            elif errors:
                if canonical is not None:
                    st.session_state["last_params_canonical"] = canonical
                if mtime is not None:
                    st.session_state["last_params_mtime"] = mtime
                st.session_state["params_validation_errors"] = errors
                st.error(t("msg.params_validation_error"))
            else:
                st.warning(t("msg.params_not_found"))

# Auto-execute check
auto_execute_pending = st.session_state.get("auto_execute_pending", False)
if auto_execute_pending:
    st.session_state["auto_execute_pending"] = False
    if not st.session_state.get("is_executing", False):
        st.session_state["execute_requested"] = True
        st.session_state["is_executing"] = True
        st.rerun()

# BigQuery SQL input area (main area)
if source == "BigQuery":
    st.subheader(t("bq.sql_header"))

    sample_sql = """SELECT
    event_date,
    COUNT(*) as event_count
FROM `project.analytics_123456789.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260101' AND '20260131'
GROUP BY event_date
ORDER BY event_date"""

    if "w_bq_sql" not in st.session_state:
        st.session_state["w_bq_sql"] = lp.get("sql", sample_sql) if lp.get("source", "").lower() == "bigquery" else sample_sql

    sql = st.text_area(t("bq.sql"), height=200, key="w_bq_sql")

    # Dataset list
    if bq_project:
        with st.expander(t("bq.datasets")):
            try:
                datasets = get_bq_datasets(bq_project)
                if datasets:
                    st.write(", ".join(datasets))
                else:
                    st.info(t("bq.no_datasets"))
            except Exception as e:
                st.warning(t("bq.dataset_error", error=str(e)))

# Main area
if st.session_state.get("execute_requested") and st.session_state.get("is_executing"):
    with st.spinner(t("msg.fetching")):
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
            elif source == "GSC":
                gsc_dimension_filter = parse_gsc_filter(gsc_filter) if 'gsc_filter' in dir() else None
                df = execute_gsc_query(
                    site_url,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    dimensions,
                    limit,
                    gsc_dimension_filter
                )
            elif source == "AA":
                if not company_id.strip():
                    st.error(t("msg.enter_aa_company_id"))
                    df = None
                elif not rsid.strip():
                    st.error(t("msg.enter_aa_rsid"))
                    df = None
                elif not dimension.strip():
                    st.error(t("msg.enter_aa_dimension"))
                    df = None
                elif not metrics:
                    st.error(t("msg.enter_aa_metrics"))
                    df = None
                else:
                    df = execute_aa_query(
                        company_id.strip(),
                        rsid.strip(),
                        start_date.strftime("%Y-%m-%d"),
                        end_date.strftime("%Y-%m-%d"),
                        dimension.strip(),
                        metrics,
                        aa_segments if 'aa_segments' in dir() else [],
                        limit,
                        org_id.strip() if 'org_id' in dir() else "",
                    )
            else:
                # BigQuery
                if not bq_project:
                    st.error(t("msg.enter_project_id"))
                    df = None
                elif not sql.strip():
                    st.error(t("msg.enter_sql"))
                    df = None
                else:
                    df = execute_bq_query(bq_project, sql)

            if df is not None and not df.empty:
                st.success(t("msg.rows_fetched", count=f"{len(df):,}"))
                st.session_state["df"] = df
            elif df is not None:
                st.warning(t("msg.no_data"))

        except Exception as e:
            st.error(t("msg.error", error=str(e)))
        finally:
            st.session_state["execute_requested"] = False
            st.session_state["is_executing"] = False

# Results display
if "df" in st.session_state:
    raw_df = st.session_state["df"]

    # === Pipeline UI ===
    with st.expander(t("pipeline.header"), expanded=False):
        # --- Transform ---
        st.markdown(t("pipeline.transform"))
        has_date_col = "date" in raw_df.columns
        url_cols = detect_url_columns(raw_df)
        has_url_col = len(url_cols) > 0

        pcol1, pcol2 = st.columns(2)
        keep_params = ""
        with pcol1:
            tf_date = st.checkbox(
                t("pipeline.tf_date"),
                disabled=not has_date_col,
                key="w_tf_date",
            )

            tf_url_decode = st.checkbox(
                t("pipeline.tf_url_decode"),
                disabled=not has_url_col,
                key="w_tf_url_decode",
            )

        with pcol2:
            tf_strip_qs = st.checkbox(
                t("pipeline.tf_strip_qs"),
                disabled=not has_url_col,
                key="w_tf_strip_qs",
            )
            if tf_strip_qs and url_cols:
                keep_params = st.text_input(
                    t("pipeline.tf_keep_params"),
                    key="w_tf_keep_params",
                    placeholder="id,ref",
                )

            tf_path_only = st.checkbox(
                t("pipeline.tf_path_only"),
                disabled=not has_url_col,
                key="w_tf_path_only",
            )
        transform_expr = build_transform_expression(
            has_date_col=has_date_col,
            url_cols=url_cols,
            tf_date=tf_date,
            tf_url_decode=tf_url_decode,
            tf_strip_qs=tf_strip_qs,
            keep_params=keep_params,
            tf_path_only=tf_path_only,
        )

        st.divider()

        # --- Filter ---
        st.markdown(t("pipeline.filter"))
        where_expr = st.text_input(
            t("pipeline.where"),
            key="w_pipeline_where",
            placeholder='clicks > 100 and page.str.contains("/blog/")',
        )

        st.divider()

        # --- Columns ---
        st.markdown(t("pipeline.columns_header"))
        selected_cols = st.multiselect(
            t("pipeline.columns"),
            list(raw_df.columns),
            key="w_pipeline_columns",
        )

        st.divider()

        # --- Group & Aggregate ---
        st.markdown(t("pipeline.group_header"))
        group_cols = st.multiselect(
            t("pipeline.group_cols"),
            list(raw_df.columns),
            key="w_pipeline_group_by",
        )
        numeric_cols = list(raw_df.select_dtypes(include="number").columns)
        agg_map = {}
        if group_cols and numeric_cols:
            st.caption(t("pipeline.agg_caption"))
            agg_funcs = [AGG_NONE, "sum", "mean", "count", "min", "max", "median"]
            for nc in numeric_cols:
                agg_func = st.selectbox(
                    f"{nc}",
                    agg_funcs,
                    format_func=lambda x: t("agg.none") if x == AGG_NONE else x,
                    key=f"w_agg_{nc}",
                )
                agg_map[nc] = agg_func

        st.divider()

        # --- Row limit ---
        st.markdown(t("pipeline.head_header"))
        head_val = st.slider(
            t("pipeline.head"),
            min_value=0,
            max_value=min(len(raw_df), 10000),
            value=0,
            step=10,
            key="w_pipeline_head",
        )

        pipeline_kwargs, derived_cols = build_pipeline_kwargs(
            transform_expr=transform_expr,
            where_expr=where_expr,
            selected_cols=selected_cols,
            group_cols=group_cols,
            agg_map=agg_map,
            head_val=head_val,
        )

        if "group_by" in pipeline_kwargs and "aggregate" in pipeline_kwargs:
            st.caption(t("pipeline.derived_cols", cols=", ".join(group_cols + derived_cols)))

    # === Pipeline apply ===
    pipeline_error = None
    if pipeline_kwargs:
        try:
            display_df = apply_pipeline(raw_df, **pipeline_kwargs)
        except ValueError as e:
            pipeline_error = str(e)
            display_df = raw_df
    else:
        display_df = raw_df

    if pipeline_error:
        st.error(t("msg.pipeline_error", error=pipeline_error))

    # Row count caption
    if len(display_df) != len(raw_df):
        st.caption(t("msg.rows_filtered", total=f"{len(raw_df):,}", filtered=f"{len(display_df):,}"))
    else:
        st.caption(t("msg.rows_summary", total=f"{len(display_df):,}"))

    # Tabs
    tab1, tab2, tab3 = st.tabs(t_options(["tab.table", "tab.chart", "tab.save"]))

    with tab1:
        st.dataframe(display_df, width="stretch", height=400)

        # Statistics
        with st.expander(t("table.stats")):
            st.write(display_df.describe())

    with tab2:
        if len(display_df.columns) >= 2:
            chart_df = display_df.copy()
            auto_x_col, auto_x_values = detect_datetime_x_axis(chart_df)
            if auto_x_col and auto_x_values is not None:
                chart_df[auto_x_col] = auto_x_values
                chart_df = chart_df[chart_df[auto_x_col].notna()].copy()

            if chart_df.empty:
                st.warning(t("msg.no_data"))
            else:
                col1, col2, col3 = st.columns(3)
                with col1:
                    x_options = list(chart_df.columns)
                    default_x_index = x_options.index(auto_x_col) if auto_x_col in x_options else 0
                    x_col = st.selectbox(t("chart.x_axis"), x_options, index=default_x_index)
                with col2:
                    y_candidates = [
                        c for c in chart_df.columns
                        if c != x_col and pd.api.types.is_numeric_dtype(chart_df[c])
                    ]
                    if y_candidates:
                        y_col = st.selectbox(t("chart.y_axis"), y_candidates)
                    else:
                        y_col = None
                        st.selectbox(t("chart.y_axis"), ["-"], disabled=True)
                with col3:
                    series_none_label = t("chart.series_none")
                    if y_col:
                        series_candidates = [
                            c for c in chart_df.columns
                            if c not in {x_col, y_col}
                            and not pd.api.types.is_numeric_dtype(chart_df[c])
                            and not pd.api.types.is_datetime64_any_dtype(chart_df[c])
                        ]
                        series_options = [series_none_label, *series_candidates]
                        default_series_index = 1 if len(series_options) > 1 else 0
                        series_label = st.selectbox(
                            t("chart.series"),
                            series_options,
                            index=default_series_index,
                        )
                        series_col = None if series_label == series_none_label else series_label
                    else:
                        series_col = None
                        st.selectbox(t("chart.series"), [series_none_label], disabled=True)

                chart_opts = t_option_map({
                    "chart.line": "line",
                    "chart.bar": "bar",
                })
                chart_label = st.radio(t("chart.type"), list(chart_opts.keys()), horizontal=True)
                chart_type = chart_opts[chart_label]

                if y_col:
                    plot_df = chart_df.dropna(subset=[x_col, y_col]).copy()
                    if pd.api.types.is_datetime64_any_dtype(plot_df[x_col]):
                        plot_df = plot_df.sort_values(x_col)

                    if series_col:
                        series_limit = 20
                        plot_df["_series_key"] = plot_df[series_col].astype("string").fillna("(null)")
                        total_series = int(plot_df["_series_key"].nunique(dropna=False))
                        if total_series > series_limit:
                            top_keys = plot_df["_series_key"].value_counts(dropna=False).head(series_limit).index
                            plot_df = plot_df[plot_df["_series_key"].isin(top_keys)].copy()
                            st.caption(
                                t(
                                    "chart.series_trimmed",
                                    shown=f"{series_limit}",
                                    total=f"{total_series:,}",
                                )
                            )
                        data_for_chart = plot_df.pivot_table(
                            index=x_col,
                            columns="_series_key",
                            values=y_col,
                            aggfunc="sum",
                        )
                    else:
                        data_for_chart = plot_df.set_index(x_col)[y_col]

                    if chart_type == "line":
                        st.line_chart(data_for_chart)
                    else:
                        st.bar_chart(data_for_chart)

    with tab3:
        st.subheader(t("save.local_header"))
        save_filename = st.text_input(
            t("save.filename"),
            value=f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            key="w_save_filename",
        )
        col1, col2 = st.columns(2)
        with col1:
            csv = display_df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                t("save.csv_download"),
                csv,
                save_filename,
                "text/csv",
                width="stretch"
            )
        with col2:
            if st.button(t("save.save_to_output"), width="stretch"):
                os.makedirs("output", exist_ok=True)
                filepath = f"output/{save_filename}"
                display_df.to_csv(filepath, index=False, encoding='utf-8-sig')
                st.success(t("save.saved", path=filepath))

        st.divider()
        st.subheader(t("save.sheets_header"))

        sheet_url = st.text_input(
            t("save.sheet_url"),
            placeholder="https://docs.google.com/spreadsheets/d/xxxxx",
            key="w_sheet_url"
        )

        col1, col2 = st.columns(2)
        with col1:
            sheet_name = st.text_input(t("save.sheet_name"), value="data", key="w_sheet_name")
        with col2:
            save_mode_labels, save_mode_default_idx, save_mode_opts = translated_select_model(
                SAVE_MODE_KEYS,
                current_value=st.session_state.get("w_save_mode", "overwrite"),
            )
            save_mode_label = st.selectbox(
                t("save.mode"),
                save_mode_labels,
                index=save_mode_default_idx,
                key="w_save_mode_select",
            )
            save_mode = save_mode_opts[save_mode_label]
            st.session_state["w_save_mode"] = save_mode

        # Upsert key columns
        if save_mode == "upsert":
            key_cols = st.multiselect(t("save.upsert_keys"), display_df.columns.tolist(), key="w_upsert_keys")

        if st.button(t("save.sheets_button"), width="stretch", type="primary"):
            if not sheet_url:
                st.error(t("save.enter_sheet_url"))
            else:
                try:
                    if save_mode == "upsert" and not key_cols:
                        st.error(t("save.select_keys"))
                    else:
                        save_to_sheet(sheet_url, sheet_name, display_df, mode=save_mode, keys=key_cols if save_mode == "upsert" else None)
                        st.success(t("save.sheets_saved", name=sheet_name))
                except Exception as e:
                    st.error(t("msg.error", error=str(e)))

        st.divider()
        st.subheader(t("save.bq_header"))

        bq_project = st.text_input(
            t("save.bq_project"),
            key="w_save_bq_project",
            placeholder="my-project-id",
        )
        col1, col2 = st.columns(2)
        with col1:
            bq_dataset = st.text_input(t("save.bq_dataset"), key="w_save_bq_dataset")
        with col2:
            bq_table = st.text_input(t("save.bq_table"), key="w_save_bq_table")

        bq_mode_labels, bq_mode_default_idx, bq_mode_opts = translated_select_model(
            SAVE_BQ_MODE_KEYS,
            current_value=st.session_state.get("w_save_bq_mode", "overwrite"),
        )
        bq_mode_label = st.selectbox(
            t("save.bq_mode"),
            bq_mode_labels,
            index=bq_mode_default_idx,
            key="w_save_bq_mode_select",
        )
        bq_mode = bq_mode_opts[bq_mode_label]
        st.session_state["w_save_bq_mode"] = bq_mode

        if st.button(t("save.bq_button"), width="stretch", type="primary"):
            if not all([bq_project, bq_dataset, bq_table]):
                st.error(t("save.bq_enter_all"))
            else:
                try:
                    save_to_bq(bq_project, bq_dataset, bq_table, display_df, mode=bq_mode)
                    st.success(t("save.bq_saved", dest=f"{bq_project}.{bq_dataset}.{bq_table}"))
                except Exception as e:
                    st.error(t("msg.error", error=str(e)))

# JSON params display (AI Agent integration)
with st.sidebar:
    with st.expander(t("agent.json_header")):
        params = build_agent_params(
            source=source,
            start_date=start_date if 'start_date' in dir() else None,
            end_date=end_date if 'end_date' in dir() else None,
            limit=limit if 'limit' in dir() else None,
            property_id=property_id if 'property_id' in dir() else "",
            site_url=site_url if 'site_url' in dir() else "",
            dimensions=dimensions if 'dimensions' in dir() else [],
            metrics=metrics if 'metrics' in dir() else [],
            filter_d=filter_d if 'filter_d' in dir() else "",
            gsc_filter=gsc_filter if 'gsc_filter' in dir() else "",
            aa_company_id=company_id if 'company_id' in dir() else "",
            aa_rsid=rsid if 'rsid' in dir() else "",
            aa_dimension=dimension if 'dimension' in dir() else "",
            aa_metrics=metrics if source == "AA" and 'metrics' in dir() else [],
            aa_segment=aa_segments if 'aa_segments' in dir() else [],
            bq_project=bq_project if 'bq_project' in dir() else "",
            sql=sql if 'sql' in dir() else "",
        )
        st.code(json.dumps(params, indent=2, ensure_ascii=False), language="json")
