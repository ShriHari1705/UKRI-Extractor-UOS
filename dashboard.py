"""
dashboard.py — UKRI Early Warning System · Streamlit Dashboard

Data source: Snowflake  UKRI_EWS.RAW_MARTS.MART_KEYWORD_TAGS

Run locally:  streamlit run dashboard.py
Deploy:       push to GitHub → Streamlit Community Cloud → share as a link
Credentials:  .streamlit/secrets.toml  (gitignored)
"""
import os
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="UKRI Early Warning System",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

PRIORITY_ORDER = ["Critical", "High", "Medium", "Low"]
PRIORITY_COLOURS = {
    "Critical": "#d62728",
    "High":     "#ff7f0e",
    "Medium":   "#1f77b4",
    "Low":      "#2ca02c",
}

DB   = "UKRI_EWS"
SCH  = "RAW_MARTS"
LONG = f"{DB}.{SCH}.MART_KEYWORD_TAGS"
PROJ = f"{DB}.{SCH}.MART_EARLY_WARNING_SIGNAL"

# ── Snowflake connection ───────────────────────────────────────────────────────

def _sf_param(key: str, env_key: str) -> str:
    try:
        return st.secrets["snowflake"][key]
    except (KeyError, FileNotFoundError):
        return os.environ.get(env_key, "")


@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_connection():
    try:
        import snowflake.connector
    except ImportError:
        st.error("Install: pip install 'snowflake-connector-python[pandas]'")
        st.stop()

    return snowflake.connector.connect(
        account=_sf_param("account",    "SNOWFLAKE_ACCOUNT"),
        user=_sf_param("user",          "SNOWFLAKE_USER"),
        password=_sf_param("password",  "SNOWFLAKE_PASSWORD"),
        warehouse=_sf_param("warehouse","SNOWFLAKE_WAREHOUSE"),
        database=DB,
        schema=SCH,
    )


@st.cache_data(ttl=600, show_spinner="Loading keyword tags…")
def load_long() -> pd.DataFrame:
    """Long format — one row per (project × keyword)."""
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {LONG}", conn)
    df.columns = df.columns.str.lower()
    df["start_date"]  = pd.to_datetime(df["start_date"],  errors="coerce")
    df["end_date"]    = pd.to_datetime(df["end_date"],     errors="coerce")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"],  errors="coerce")
    return df


@st.cache_data(ttl=600, show_spinner="Loading project signals…")
def load_projects() -> pd.DataFrame:
    """One row per project from mart_early_warning_signal."""
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {PROJ}", conn)
    df.columns = df.columns.str.lower()
    df["start_date"]    = pd.to_datetime(df["start_date"],    errors="coerce")
    df["end_date"]      = pd.to_datetime(df["end_date"],      errors="coerce")
    df["ingested_at"]   = pd.to_datetime(df["ingested_at"],   errors="coerce")
    df["starting_soon"] = df["starting_soon"].astype(bool)
    df["start_month"]   = df["start_date"].dt.to_period("M").astype(str)
    return df


# ── Load ──────────────────────────────────────────────────────────────────────
long_df = load_long()
proj_df = load_projects()

if proj_df.empty:
    st.warning("No data returned from Snowflake. Has the pipeline run yet?")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📡 EWS Filters")
    last_ingested = proj_df["ingested_at"].max()
    st.caption(f"Last ingested: {last_ingested.strftime('%Y-%m-%d %H:%M') if pd.notna(last_ingested) else 'unknown'}")

    sel_priority = st.multiselect("Priority", PRIORITY_ORDER, default=PRIORITY_ORDER)

    all_cats = sorted(long_df["category"].unique())
    sel_category = st.multiselect("Category", all_cats, default=all_cats)

    all_statuses = sorted(proj_df["status"].dropna().unique())
    sel_status = st.multiselect("Status", all_statuses, default=all_statuses)

    all_grants = sorted(proj_df["grant_category"].dropna().unique())
    sel_grant = st.multiselect("Grant category", all_grants, default=all_grants)

    min_d = proj_df["start_date"].min().date() if not proj_df["start_date"].isna().all() else date(2020, 1, 1)
    max_d = proj_df["start_date"].max().date() if not proj_df["start_date"].isna().all() else date.today()
    date_range = st.date_input("Start date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
    if len(date_range) == 2:
        date_from, date_to = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    else:
        date_from, date_to = pd.Timestamp(min_d), pd.Timestamp(max_d)

    st.divider()
    only_alerts = st.toggle("Active alerts only (±1yr / +90d)", value=False)
    st.divider()
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

# ── Apply filters ─────────────────────────────────────────────────────────────
ids_with_cat = long_df[long_df["category"].isin(sel_category)]["project_id"].unique()

mask = (
    proj_df["priority"].isin(sel_priority)
    & proj_df["status"].isin(sel_status)
    & proj_df["grant_category"].isin(sel_grant)
    & proj_df["start_date"].between(date_from, date_to, inclusive="both")
    & proj_df["project_id"].isin(ids_with_cat)
)
if only_alerts:
    mask &= proj_df["starting_soon"].astype(bool)

filtered = proj_df[mask].copy()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📡 UKRI Early Warning System")
st.caption("Innovate UK projects flagged for compute / cloud / AI / data-intensive activity · Source: Snowflake")

if filtered.empty:
    st.warning("No projects match the current filters.")
    st.stop()

# ── KPI row ───────────────────────────────────────────────────────────────────
total     = len(filtered)
alerts    = int(filtered["starting_soon"].sum())
high_crit = int(filtered["priority"].isin(["Critical", "High"]).sum())
new_30    = int((filtered["ingested_at"] >= pd.Timestamp(date.today() - timedelta(days=30))).sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("Projects matched", f"{total:,}")
k2.metric("Active alerts", f"{alerts:,}", help="Started within last 365 days or starting within next 90 days")
k3.metric("High / Critical", f"{high_crit:,}")
k4.metric("Ingested last 30 days", f"{new_30:,}")

st.divider()

# ── Row 1: Category breakdown + Priority distribution ─────────────────────────
col1, col2 = st.columns([3, 2])

with col1:
    cat_counts = (
        long_df[
            long_df["project_id"].isin(filtered["project_id"])
            & long_df["category"].isin(sel_category)
        ]
        .groupby("category")["project_id"]
        .nunique()
        .reset_index(name="projects")
        .sort_values("projects", ascending=True)
    )
    fig_cat = px.bar(
        cat_counts, x="projects", y="category", orientation="h",
        title="Projects by technology category",
        labels={"projects": "Unique projects", "category": ""},
        color="category",
        color_discrete_sequence=px.colors.qualitative.Safe,
    )
    fig_cat.update_layout(showlegend=False, height=300, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_cat, use_container_width=True)

with col2:
    pri_counts = (
        filtered.groupby("priority").size()
        .reindex(PRIORITY_ORDER, fill_value=0)
        .reset_index(name="count")
    )
    fig_pri = px.pie(
        pri_counts, names="priority", values="count",
        title="Priority distribution",
        color="priority", color_discrete_map=PRIORITY_COLOURS,
        hole=0.45,
    )
    fig_pri.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_pri, use_container_width=True)

# ── Row 2: Project timeline ───────────────────────────────────────────────────
monthly = (
    filtered.dropna(subset=["start_date"])
    .groupby("start_month").size()
    .reset_index(name="projects")
    .sort_values("start_month")
)
if not monthly.empty:
    fig_time = px.bar(
        monthly, x="start_month", y="projects",
        title="Projects by start month",
        labels={"start_month": "Month", "projects": "Projects"},
    )
    fig_time.update_layout(height=280, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_time, use_container_width=True)

# ── Row 3: Category co-occurrence ────────────────────────────────────────────
with st.expander("Category co-occurrence (which categories appear together)", expanded=False):
    cats = sorted(long_df["category"].unique())
    matrix = pd.DataFrame(0, index=cats, columns=cats)
    for _, row in filtered.iterrows():
        flagged = [c.strip() for c in str(row["categories_flagged"]).split(",") if c.strip() in cats]
        for a in flagged:
            for b in flagged:
                matrix.loc[a, b] += 1

    fig_heat = px.imshow(
        matrix, text_auto=True,
        title="Category co-occurrence (projects flagged for both)",
        color_continuous_scale="Blues",
    )
    fig_heat.update_layout(height=380, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_heat, use_container_width=True)

# ── Row 4: Top keywords ───────────────────────────────────────────────────────
with st.expander("Top keywords (across filtered projects)", expanded=True):
    kw_counts = (
        long_df[
            long_df["project_id"].isin(filtered["project_id"])
            & long_df["category"].isin(sel_category)
        ]
        .groupby(["category", "keyword"])["project_id"]
        .nunique()
        .reset_index(name="projects")
        .sort_values("projects", ascending=False)
        .head(30)
    )
    fig_kw = px.bar(
        kw_counts.sort_values("projects"), x="projects", y="keyword",
        color="category", orientation="h",
        title="Top 30 keywords by unique project count",
        labels={"projects": "Projects", "keyword": ""},
        color_discrete_sequence=px.colors.qualitative.Safe,
        height=600,
    )
    fig_kw.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_kw, use_container_width=True)

# ── Project table ─────────────────────────────────────────────────────────────
st.subheader("Project list")

display = filtered[[
    "priority", "compute_score", "title", "status",
    "grant_category", "start_date", "end_date",
    "categories_flagged", "keywords_matched", "gtr_url",
]].sort_values(["compute_score", "start_date"], ascending=[False, False])


def colour_priority(val):
    c = PRIORITY_COLOURS.get(val, "#888888")
    return f"background-color: {c}22; color: {c}; font-weight: bold"


st.dataframe(
    display.style.map(colour_priority, subset=["priority"]),
    use_container_width=True,
    hide_index=True,
    column_config={
        "gtr_url":            st.column_config.LinkColumn("GTR link", display_text="Open"),
        "start_date":         st.column_config.DateColumn("Start date"),
        "end_date":           st.column_config.DateColumn("End date"),
        "compute_score":      st.column_config.NumberColumn("Score", format="%d / 4"),
        "priority":           "Priority",
        "title":              "Project title",
        "status":             "Status",
        "grant_category":     "Grant category",
        "categories_flagged": "Categories",
        "keywords_matched":   "Keywords",
    },
    height=500,
)

st.caption(
    f"Showing {len(filtered):,} of {len(proj_df):,} projects · "
    f"Refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
)
