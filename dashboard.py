"""
dashboard.py — UKRI Early Warning System · Streamlit Dashboard

Data source: Snowflake  UKRI_EWS.RAW_MARTS.MART_KEYWORD_TAGS

Run locally:  streamlit run dashboard.py
Deploy:       push to GitHub → Streamlit Community Cloud → share as a link
Credentials:  .streamlit/secrets.toml  (gitignored)
"""
import html
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
RPI  = f"{DB}.{SCH}.MART_RPI_FUNDING_LANDSCAPE"

# ── Snowflake connection ───────────────────────────────────────────────────────

def _sf_param(key: str, env_key: str) -> str:
    try:
        return st.secrets["snowflake"][key]
    except (KeyError, FileNotFoundError):
        return os.environ.get(env_key, "")


def _is_connection_alive(conn) -> bool:
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False


@st.cache_resource(show_spinner="Connecting to Snowflake…", validate=_is_connection_alive)
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


def _query(sql: str) -> pd.DataFrame:
    """Run a query, refreshing the connection once if the token has expired."""
    try:
        return pd.read_sql(sql, get_connection())
    except Exception as e:
        if "390114" in str(e) or "Authentication token has expired" in str(e):
            get_connection.clear()
            return pd.read_sql(sql, get_connection())
        raise


@st.cache_data(ttl=600, show_spinner="Loading keyword tags…")
def load_long() -> pd.DataFrame:
    """Long format — one row per (project × keyword)."""
    df = _query(f"SELECT * FROM {LONG}")
    df.columns = df.columns.str.lower()
    df["start_date"]  = pd.to_datetime(df["start_date"],  errors="coerce")
    df["end_date"]    = pd.to_datetime(df["end_date"],     errors="coerce")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"],  errors="coerce")
    return df


@st.cache_data(ttl=600, show_spinner="Loading project signals…")
def load_projects() -> pd.DataFrame:
    """One row per project from mart_early_warning_signal."""
    df = _query(f"SELECT * FROM {PROJ}")
    df.columns = df.columns.str.lower()
    df["start_date"]    = pd.to_datetime(df["start_date"],    errors="coerce")
    df["end_date"]      = pd.to_datetime(df["end_date"],      errors="coerce")
    df["ingested_at"]   = pd.to_datetime(df["ingested_at"],   errors="coerce")
    df["starting_soon"] = df["starting_soon"].astype(bool)
    df["start_month"]   = df["start_date"].dt.to_period("M").astype(str)
    # Normalise legacy URLs (?ref=UUID → /project/UUID)
    df["gtr_url"] = df["project_id"].apply(
        lambda pid: f"https://gtr.ukri.org/project/{pid}"
    )
    return df


@st.cache_data(ttl=600, show_spinner="Loading RPI funding landscape…")
def load_rpi() -> pd.DataFrame:
    """One row per project across all UKRI funders."""
    try:
        df = _query(f"SELECT * FROM {RPI}")
    except Exception:
        return pd.DataFrame()
    df.columns = df.columns.str.lower()
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce")
    df["start_year"] = pd.to_numeric(df.get("start_year"), errors="coerce")
    return df


# ── Load ──────────────────────────────────────────────────────────────────────
long_df = load_long()
proj_df = load_projects()
rpi_df  = load_rpi()

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

tab_ews, tab_rpi = st.tabs(["IT Services — Early Warning", "RPI Growth Manager"])

# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — IT Services / Early Warning (existing view)
# ════════════════════════════════════════════════════════════════════════════════
with tab_ews:
    st.caption("Innovate UK projects flagged for compute / cloud / AI / data-intensive activity · Source: Snowflake")

    with st.expander("How scoring works", expanded=False):
        st.markdown("""
**Compute Score (0 – 4)**

Each project's abstract, technical abstract, and potential impact text is scanned
against a keyword taxonomy covering four technology categories:

| Category | Examples |
|---|---|
| **ML_AI** | machine learning, neural network, LLM, computer vision, PyTorch |
| **HPC_Simulation** | HPC, CUDA, MPI, molecular dynamics, finite element |
| **Data_Intensive** | genomics, bioinformatics, real-time data, data pipeline, IoT |
| **Cloud_Infrastructure** | AWS, Kubernetes, Docker, Terraform, serverless |

The **Compute Score** is the number of distinct categories triggered (0 – 4).
A project mentioning 10 ML keywords still scores 1 — breadth across categories matters, not keyword frequency.

---

**Priority Tier**

| Score | Priority | Meaning |
|---|---|---|
| 4 | 🔴 Critical | All four categories flagged — significant cross-domain compute demand |
| 3 | 🟠 High | Three categories — likely needs dedicated infrastructure planning |
| 2 | 🔵 Medium | Two categories — worth monitoring |
| 1 | 🟢 Low | Single category signal |

---

**Active Alert flag**

A project is marked as an active alert if its start date falls within the window:
**365 days in the past → 90 days in the future** relative to today.
This catches recently-started projects (already drawing on resources) and
upcoming ones (capacity planning needed soon).
        """)

    if filtered.empty:
        st.warning("No projects match the current filters.")
    else:
        # ── KPI row ───────────────────────────────────────────────────────────
        total     = len(filtered)
        alerts    = int(filtered["starting_soon"].sum())
        high_crit = int(filtered["priority"].isin(["Critical", "High"]).sum())
        cutoff_30 = pd.Timestamp(date.today() - timedelta(days=30), tz="UTC")
        new_30    = int((filtered["ingested_at"].dt.tz_convert("UTC") >= cutoff_30).sum())

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Projects matched", f"{total:,}")
        k2.metric("Active alerts", f"{alerts:,}", help="Started within last 365 days or starting within next 90 days")
        k3.metric("High / Critical", f"{high_crit:,}")
        k4.metric("Ingested last 30 days", f"{new_30:,}")

        st.divider()

        # ── Category breakdown + Priority distribution ────────────────────────
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

        # ── Project timeline ──────────────────────────────────────────────────
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

        # ── Category co-occurrence ────────────────────────────────────────────
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

        # ── Top keywords ──────────────────────────────────────────────────────
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

        # ── Project table ─────────────────────────────────────────────────────
        st.subheader("Project list")
        display = filtered[[
            "priority", "compute_score", "title", "status",
            "grant_category", "start_date", "end_date",
            "categories_flagged", "keywords_matched", "gtr_url",
        ]].sort_values(["compute_score", "start_date"], ascending=[False, False])

        def _project_table_html(df: pd.DataFrame) -> str:
            css = """
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0 }
:root {
  --bg:#ffffff;--bg-alt:#f7f8fa;--bg-hover:#eef1f7;--border:#e2e5eb;
  --text:#1a1d23;--muted:#5f6775;--link:#1558d6;--link-hov:#0d3fa0;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg:#0e1117;--bg-alt:#161b22;--bg-hover:#1c2333;--border:#2d3340;
    --text:#cdd5e0;--muted:#7c8794;--link:#58a6ff;--link-hov:#79b8ff;
  }
}
html,body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;font-size:13px;line-height:1.5}
.wrap{overflow-x:auto;max-height:560px;overflow-y:auto}
table{width:100%;border-collapse:collapse;table-layout:fixed}
col.c-pri{width:88px}col.c-score{width:56px}col.c-title{width:26%}
col.c-status{width:78px}col.c-grant{width:13%}col.c-date{width:82px}
col.c-cats{width:18%}col.c-kw{width:auto}
thead th{padding:8px 10px;text-align:left;background:var(--bg-alt);border-bottom:2px solid var(--border);
  color:var(--muted);font-size:10.5px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;
  white-space:nowrap;position:sticky;top:0;z-index:1}
tbody tr{border-bottom:1px solid var(--border)}
tbody tr:last-child{border-bottom:none}
tbody tr:hover td{background:var(--bg-hover)}
td{padding:9px 10px;vertical-align:top;word-break:break-word}
a{color:var(--link);text-decoration:none;font-weight:500}
a:hover{color:var(--link-hov);text-decoration:underline}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;letter-spacing:.02em;white-space:nowrap}
.muted{color:var(--muted)}.mono{font-variant-numeric:tabular-nums;white-space:nowrap}
.sm{font-size:12px}.xs{font-size:11px}
</style>"""
            colgroup = (
                '<colgroup>'
                '<col class="c-pri"><col class="c-score"><col class="c-title">'
                '<col class="c-status"><col class="c-grant">'
                '<col class="c-date"><col class="c-date">'
                '<col class="c-cats"><col class="c-kw">'
                '</colgroup>'
            )
            thead = (
                '<thead><tr>'
                '<th>Priority</th><th>Score</th><th>Project title</th>'
                '<th>Status</th><th>Grant category</th><th>Start</th><th>End</th>'
                '<th>Categories</th><th>Keywords</th>'
                '</tr></thead>'
            )
            rows = []
            for _, row in df.iterrows():
                pc    = PRIORITY_COLOURS.get(row["priority"], "#888")
                start = row["start_date"].strftime("%Y-%m-%d") if pd.notna(row["start_date"]) else "—"
                end   = row["end_date"].strftime("%Y-%m-%d")   if pd.notna(row["end_date"])   else "—"
                title = html.escape(str(row["title"]))
                url   = html.escape(str(row["gtr_url"]))
                pri   = html.escape(str(row["priority"]))
                rows.append(
                    f'<tr>'
                    f'<td><span class="badge" style="background:{pc}1a;color:{pc};border:1px solid {pc}55">{pri}</span></td>'
                    f'<td class="muted mono sm" style="text-align:center">{int(row["compute_score"])} / 4</td>'
                    f'<td><a href="{url}" target="_blank">{title}</a></td>'
                    f'<td class="muted sm">{html.escape(str(row["status"]))}</td>'
                    f'<td class="muted sm">{html.escape(str(row["grant_category"]))}</td>'
                    f'<td class="muted mono sm">{start}</td>'
                    f'<td class="muted mono sm">{end}</td>'
                    f'<td class="muted sm">{html.escape(str(row["categories_flagged"]))}</td>'
                    f'<td class="muted xs">{html.escape(str(row["keywords_matched"]))}</td>'
                    f'</tr>'
                )
            return (
                css
                + '<div class="wrap">'
                + f'<table>{colgroup}{thead}<tbody>'
                + "\n".join(rows)
                + "</tbody></table></div>"
            )

        st.html(_project_table_html(display))
        st.caption(
            f"Showing {len(filtered):,} of {len(proj_df):,} projects · "
            f"Refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )

# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — RPI Growth Manager
# ════════════════════════════════════════════════════════════════════════════════
with tab_rpi:
    st.caption("UKRI funding landscape across all research councils · For Research & Policy Intelligence teams")

    if rpi_df.empty:
        st.info(
            "No RPI data yet. Run the pipeline to populate it:\n\n"
            "```\npython run_rpi_pipeline.py --snowflake --overwrite\n"
            "dbt run --select mart_rpi_funding_landscape\n```"
        )
    else:
        # ── Sidebar filters for RPI tab ───────────────────────────────────────
        # (sidebar is shared — RPI uses its own in-tab filters to avoid conflict)
        all_funders = sorted(rpi_df["lead_funder"].dropna().unique())
        rpi_sel_funders = st.multiselect(
            "Filter by funder", all_funders, default=all_funders, key="rpi_funders"
        )
        rpi_col1, rpi_col2 = st.columns(2)
        with rpi_col1:
            all_rpi_grants = sorted(rpi_df["grant_category"].dropna().unique())
            rpi_sel_grants = st.multiselect(
                "Grant category", all_rpi_grants, default=all_rpi_grants, key="rpi_grants"
            )
        with rpi_col2:
            rpi_years = sorted(rpi_df["start_year"].dropna().astype(int).unique())
            if rpi_years:
                rpi_year_range = st.select_slider(
                    "Start year range", options=rpi_years,
                    value=(rpi_years[0], rpi_years[-1]), key="rpi_years"
                )
            else:
                rpi_year_range = (0, 9999)

        rpi_mask = (
            rpi_df["lead_funder"].isin(rpi_sel_funders)
            & rpi_df["grant_category"].isin(rpi_sel_grants)
            & rpi_df["start_year"].between(rpi_year_range[0], rpi_year_range[1])
        )
        rpi_filtered = rpi_df[rpi_mask].copy()

        if rpi_filtered.empty:
            st.warning("No projects match the selected filters.")
        else:
            # ── KPI row ───────────────────────────────────────────────────────
            rpi_total   = len(rpi_filtered)
            rpi_active  = int(rpi_filtered.get("currently_active", pd.Series(dtype=bool)).sum())
            rpi_funders = rpi_filtered["lead_funder"].nunique()
            rpi_recent  = int(rpi_filtered.get("started_last_2_years", pd.Series(dtype=bool)).sum())

            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Total projects", f"{rpi_total:,}")
            r2.metric("Currently active", f"{rpi_active:,}", help="Start ≤ today ≤ End")
            r3.metric("Funders", f"{rpi_funders:,}")
            r4.metric("Started last 2 years", f"{rpi_recent:,}")

            st.divider()

            # ── Row 1: By funder + By grant category ──────────────────────────
            rc1, rc2 = st.columns(2)

            with rc1:
                by_funder = (
                    rpi_filtered.groupby("lead_funder").size()
                    .reset_index(name="projects")
                    .sort_values("projects", ascending=True)
                )
                fig_funder = px.bar(
                    by_funder, x="projects", y="lead_funder", orientation="h",
                    title="Projects by funder",
                    labels={"projects": "Projects", "lead_funder": ""},
                    color_discrete_sequence=px.colors.qualitative.Safe,
                )
                fig_funder.update_layout(showlegend=False, height=350, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_funder, use_container_width=True)

            with rc2:
                by_grant = (
                    rpi_filtered.groupby("grant_category").size()
                    .reset_index(name="projects")
                    .sort_values("projects", ascending=False)
                    .head(12)
                )
                fig_grant = px.pie(
                    by_grant, names="grant_category", values="projects",
                    title="Projects by grant category",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig_grant.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_grant, use_container_width=True)

            # ── Row 2: Funding trend by year ──────────────────────────────────
            trend = (
                rpi_filtered.dropna(subset=["start_year"])
                .groupby(["start_year", "lead_funder"]).size()
                .reset_index(name="projects")
                .sort_values("start_year")
            )
            trend["start_year"] = trend["start_year"].astype(int)
            if not trend.empty:
                fig_trend = px.bar(
                    trend, x="start_year", y="projects", color="lead_funder",
                    title="Projects awarded per year by funder",
                    labels={"start_year": "Year", "projects": "Projects", "lead_funder": "Funder"},
                    color_discrete_sequence=px.colors.qualitative.Safe,
                )
                fig_trend.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_trend, use_container_width=True)

            # ── Project table ─────────────────────────────────────────────────
            st.subheader("Project list")
            rpi_display = rpi_filtered[[
                "lead_funder", "grant_category", "title",
                "status", "start_date", "end_date", "gtr_url",
            ]].sort_values("start_date", ascending=False)

            def _rpi_table_html(df: pd.DataFrame) -> str:
                css = """
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#fff;--bg-alt:#f7f8fa;--bg-hover:#eef1f7;--border:#e2e5eb;--text:#1a1d23;--muted:#5f6775;--link:#1558d6;--link-hov:#0d3fa0}
@media(prefers-color-scheme:dark){:root{--bg:#0e1117;--bg-alt:#161b22;--bg-hover:#1c2333;--border:#2d3340;--text:#cdd5e0;--muted:#7c8794;--link:#58a6ff;--link-hov:#79b8ff}}
html,body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;font-size:13px;line-height:1.5}
.wrap{overflow-x:auto;max-height:520px;overflow-y:auto}
table{width:100%;border-collapse:collapse;table-layout:fixed}
col.cf{width:14%}col.cg{width:14%}col.ct{width:32%}col.cs{width:80px}col.cd{width:86px}col.cdk{width:auto}
thead th{padding:8px 10px;text-align:left;background:var(--bg-alt);border-bottom:2px solid var(--border);
  color:var(--muted);font-size:10.5px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;
  white-space:nowrap;position:sticky;top:0;z-index:1}
tbody tr{border-bottom:1px solid var(--border)}tbody tr:last-child{border-bottom:none}
tbody tr:hover td{background:var(--bg-hover)}
td{padding:9px 10px;vertical-align:top;word-break:break-word}
a{color:var(--link);text-decoration:none;font-weight:500}a:hover{color:var(--link-hov);text-decoration:underline}
.muted{color:var(--muted)}.mono{font-variant-numeric:tabular-nums;white-space:nowrap}.sm{font-size:12px}
</style>"""
                colgroup = (
                    '<colgroup>'
                    '<col class="cf"><col class="cg"><col class="ct">'
                    '<col class="cs"><col class="cd"><col class="cdk">'
                    '</colgroup>'
                )
                thead = (
                    '<thead><tr>'
                    '<th>Funder</th><th>Grant category</th><th>Project title</th>'
                    '<th>Status</th><th>Start</th><th>End</th>'
                    '</tr></thead>'
                )
                rows = []
                for _, row in df.iterrows():
                    start = row["start_date"].strftime("%Y-%m-%d") if pd.notna(row["start_date"]) else "—"
                    end   = row["end_date"].strftime("%Y-%m-%d")   if pd.notna(row["end_date"])   else "—"
                    rows.append(
                        f'<tr>'
                        f'<td class="muted sm">{html.escape(str(row["lead_funder"]))}</td>'
                        f'<td class="muted sm">{html.escape(str(row["grant_category"]))}</td>'
                        f'<td><a href="{html.escape(str(row["gtr_url"]))}" target="_blank">{html.escape(str(row["title"]))}</a></td>'
                        f'<td class="muted sm">{html.escape(str(row["status"]))}</td>'
                        f'<td class="muted mono sm">{start}</td>'
                        f'<td class="muted mono sm">{end}</td>'
                        f'</tr>'
                    )
                return (
                    css
                    + '<div class="wrap">'
                    + f'<table>{colgroup}{thead}<tbody>'
                    + "\n".join(rows)
                    + "</tbody></table></div>"
                )

            st.html(_rpi_table_html(rpi_display))
            st.caption(
                f"Showing {len(rpi_filtered):,} of {len(rpi_df):,} projects across "
                f"{rpi_filtered['lead_funder'].nunique()} funders · "
                f"Refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
