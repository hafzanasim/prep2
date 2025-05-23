#streamlit_app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from data_retrieval import get_snowflake_data
from text_analysis import extract_findings, configure_gemini
from data_storage import store_data_sql, load_data_sql, init_db, reset_db, retry_failed_extractions
import sqlite3
import datetime
import io

st.set_page_config(
    page_title="Radiology Findings Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.markdown("""
<style>
.dashboard-banner {
    background-color: #006a50;
    color: white;
    padding: 1rem 1rem;
    border-radius: 10px;
    margin-bottom: 1rem;
    display: flex;
    align-items: center;
    gap: 0rem;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
}
.dashboard-logo {
    height: 100px;
    border-radius: 8px;
}
.dashboard-title {
    font-size: 2.15rem;
    font-weight: bold;
    font-family: "Times New Roman", Times, serif;
}
</style>

<div class="dashboard-banner">
    <img src="https://floridamedspace.com/wp-content/uploads/2024/05/baptist-health-logo-760x320-1-705x297.png" class="dashboard-logo" alt="Logo">
    <div class="dashboard-title">Radiology & Clinical Findings Dashboard</div>
</div>
""", unsafe_allow_html=True)


def risk_badge(level):
    color = {
        "Low": "#28a745",
        "Medium": "#ffc107",
        "High": "#dc3545"
    }.get(level, "#6c757d")

    return f'''
    <span style="
        display: inline-block;
        color: white;
        background-color: {color};
        padding: 4px 10px;
        border-radius: 6px;
        min-width: 80px;
        text-align: center;
        font-weight: bold;
    ">
        {level}
    </span>
    '''


def add_custom_css():
    st.markdown("""<style>
    .highlight { background-color: #ffeb3b; padding: 2px 0px; }

    .status-card {
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        height: 180px;
        padding: 10px;
        border-radius: 5px;
        text-align: center;
        color: white;
        font-weight: bold;
        font-size: 1.5rem;  /* Same font size for title and number */
        gap: 0.5rem;         /* Space between title and number */
    }

    .critical { background-color: #dc3545; }
    .incidental { background-color: #fd7e14; }
    .followup { background-color: #ffc107; }
    .not-needed { background-color: #28a745; }

    .radio-report-text {
        white-space: pre-wrap;
        font-family: monospace;
        font-size: 0.9rem;
        padding: 10px;
        background-color: #f9f9f9;
        border-radius: 5px;
        height: 350px;
        overflow-y: auto;
    }
    </style>""", unsafe_allow_html=True)


add_custom_css()
init_db()
configure_gemini(api_key="AIzaSyAZ5BSEUTGEOrKeX2AIUdD-CIDuH5lTB1U")

# Sidebar dev tools
if st.sidebar.button("Reset DB"):
    reset_db()
    st.sidebar.success("Database reset. Refresh to reprocess reports.")
    st.stop()

def get_radio_for_retry(empi_id, timestamp):
    return get_snowflake_data(
        user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='PUBLIC',
        query=f"""
            SELECT RADIO_REPORT_TEXT
            FROM radio_reports
            WHERE EMPI_ID = '{empi_id}'
              AND TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') = '{timestamp}'
        """
    )

def get_clinical_for_retry(empi_id, timestamp):
    df = get_snowflake_data(
        user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='PUBLIC',
        query=f"""
            SELECT CLINICAL_REPORT_TEXT
            FROM clinical_reports
            WHERE EMPI_ID = '{empi_id}'
            ORDER BY ABS(strftime('%s', '{timestamp}') - strftime('%s', TIMESTAMP)) ASC
            LIMIT 1
        """
    )
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


if st.sidebar.button("Re-run failed LLM findings"):
    updated = retry_failed_extractions(
        extract_fn=extract_findings,
        get_radio_fn=get_radio_for_retry,
        get_clinical_fn=get_clinical_for_retry
    )
    if updated:
        st.sidebar.success(f"✅ Reprocessed {updated} failed records.")
        st.rerun()
    else:
        st.sidebar.info("No failed records to reprocess.")


@st.cache_data
def load_radiology_data():
    return get_snowflake_data(
        user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='PUBLIC',
        query="SELECT EMPI_ID, RADIO_REPORT_TEXT, TIMESTAMP FROM radio_reports"
    )

@st.cache_data
def load_clinical_data():
    return get_snowflake_data(
        user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='PUBLIC',
        query="SELECT EMPI_ID, CLINICAL_REPORT_TEXT, TIMESTAMP FROM clinical_reports"
    )

# Helper to normalize timestamps
def canonical_ts(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series)
        .dt.tz_localize(None)
        .dt.floor("s")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

# Helper to match each radiology report with the closest clinical report
def merge_closest_by_timestamp(radio_df, clinical_df):
    merged_rows = []
    for _, rad_row in radio_df.iterrows():
        empi_id = rad_row["empi_id"]
        ts_rad = pd.to_datetime(rad_row["timestamp"])

        subset = clinical_df[clinical_df["empi_id"] == empi_id].copy()
        if not subset.empty:
            subset["time_diff"] = (pd.to_datetime(subset["timestamp"]) - ts_rad).abs()
            best_match = subset.sort_values("time_diff").iloc[0]
            clinical_text = best_match["CLINICAL_REPORT_TEXT"]
        else:
            clinical_text = ""

        merged_rows.append({
            "empi_id": empi_id,
            "timestamp": rad_row["timestamp"],
            "RADIO_REPORT_TEXT": rad_row["RADIO_REPORT_TEXT"],
            "CLINICAL_REPORT_TEXT": clinical_text
        })

    return pd.DataFrame(merged_rows)

# Run only once
if 'processed' not in st.session_state:
    # Load and normalize data
    radio_df = load_radiology_data()
    clinical_df = load_clinical_data()

    radio_df["timestamp"] = canonical_ts(radio_df["TIMESTAMP"])
    clinical_df["timestamp"] = canonical_ts(clinical_df["TIMESTAMP"])
    radio_df["empi_id"] = radio_df["EMPI_ID"]
    clinical_df["empi_id"] = clinical_df["EMPI_ID"]

    # Match each radiology report with the closest clinical report
    merged_df = merge_closest_by_timestamp(radio_df, clinical_df)

    # Load existing stored findings
    stored_df = load_data_sql()
    if not stored_df.empty:
        stored_df["timestamp"] = canonical_ts(stored_df["timestamp"])

    # Identify only new reports to process
    if stored_df.empty:
        new_reports = merged_df
    else:
        new_reports = merged_df.merge(
            stored_df[["empi_id", "timestamp"]].drop_duplicates(),
            on=["empi_id", "timestamp"],
            how="left",
            indicator=True
        )
        new_reports = new_reports[new_reports["_merge"] == "left_only"].drop(columns="_merge")

    # Run Gemini only on new reports
    if not new_reports.empty:
        extracted_rows = []
        for _, row in new_reports.iterrows():
            findings = extract_findings(
                radiology_text=row["RADIO_REPORT_TEXT"],
                clinical_text=row.get("CLINICAL_REPORT_TEXT", "")
            )
            extracted_rows.append({
                "empi_id": row["empi_id"],
                "timestamp": row["timestamp"],
                **findings
            })
        store_data_sql(extracted_rows)
        st.success(f"Extracted and stored {len(extracted_rows)} new findings.")
    else:
        st.info("✅ All radiology reports already processed.")

    st.session_state.processed = True


df_display = load_data_sql()

# ------------- Filters ------------------
st.markdown("### Filters")

# Defensive check for empty DataFrame
if df_display.empty:
    st.error("No data available from the database. Please check your data source or queries.")
    st.stop()

# Ensure 'timestamp' column is in datetime format
df_display['timestamp'] = pd.to_datetime(df_display['timestamp'], errors="coerce")

# Display EMPI ID options
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    empi_ids = ["All"] + sorted(df_display['empi_id'].dropna().unique())
    selected_empi = st.selectbox("Select EMPI ID", empi_ids)

with col2:
    try:
        # Ensure timestamps are datetime
        df_display['timestamp'] = pd.to_datetime(df_display['timestamp'], errors="coerce")
        valid_timestamps = df_display['timestamp'].dropna()

        if not valid_timestamps.empty:
            min_date = valid_timestamps.min().date()
            max_date = valid_timestamps.max().date()
            date_range = st.date_input("Date Range", (min_date, max_date), min_value=min_date, max_value=max_date)
        else:
            st.warning("⚠️ No valid timestamps found for filtering.")
            date_range = None
    except Exception as e:
        st.error(f"⚠️ Error parsing date range: {e}")
        date_range = None


with col3:
    selected_critical = st.selectbox("Critical Findings", ["All", "Yes", "No"])

with col4:
    selected_followup = st.selectbox("Follow-up Needed", ["All", "Yes", "No"])

with col5:
    selected_risk = st.selectbox("Risk Level", ["All", "Low", "Medium", "High"])

# Free-text search
patient_search = st.text_input("Search Patient ID")

# ------------- Filtering Logic ---------------
filtered_df = df_display.copy()

if selected_empi != "All":
    filtered_df = filtered_df[filtered_df['empi_id'] == selected_empi]

if date_range and len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        (filtered_df['timestamp'].dt.date >= start_date) &
        (filtered_df['timestamp'].dt.date <= end_date)
    ]

if selected_critical != "All":
    filtered_df = filtered_df[filtered_df['critical_findings'] == selected_critical]

if selected_followup != "All":
    filtered_df = filtered_df[filtered_df['follow_up'] == selected_followup]

if selected_risk != "All":
    filtered_df = filtered_df[filtered_df['risk_level'] == selected_risk]

if patient_search:
    filtered_df = filtered_df[filtered_df['empi_id'].str.contains(patient_search, case=False)]


# ------------- Summary Cards ------------------
st.markdown("### Findings Overview")
critical_count = (filtered_df['critical_findings'] == 'Yes').sum()
incidental_count = (filtered_df['incidental_findings'] == 'Yes').sum()
followup_count = (filtered_df['follow_up'] == 'Yes').sum()

cols = st.columns(4)
cols[0].markdown(f"""
<div class='status-card critical'>
    <div>Critical Findings</div>
    <h2>{critical_count}</h2>
</div>
""", unsafe_allow_html=True)

cols[1].markdown(f"""
<div class='status-card incidental'>
    <div>Incidental Findings</div>
    <h2>{incidental_count}</h2>
</div>
""", unsafe_allow_html=True)

cols[2].markdown(f"""
<div class='status-card followup'>
    <div>Follow-Up Required</div>
    <h2>{followup_count}</h2>
</div>
""", unsafe_allow_html=True)

cols[3].markdown(f"""
<div class='status-card not-needed'>
    <div>No Follow-Up</div>
    <h2>{len(filtered_df) - followup_count}</h2>
</div>
""", unsafe_allow_html=True)

# ------------- Pie Chart ----------------------
st.markdown("### Findings Distribution")
fig = px.pie(filtered_df, names='critical_findings', title='Critical Findings Distribution',
             color_discrete_sequence=px.colors.qualitative.Set2)
st.plotly_chart(fig, use_container_width=True)

# ------------- Paginated Table ----------------
st.markdown("### Patient List")
st.markdown(f"Showing {len(filtered_df)} patients")

ROWS_PER_PAGE = 10
total_pages = max((len(filtered_df) - 1) // ROWS_PER_PAGE + 1, 1)
st.session_state.page_num = st.session_state.get("page_num", 1)

col1, col2, col3 = st.columns([1, 3, 1])
with col2:
    page_input = st.number_input("Page", 1, total_pages, value=st.session_state.page_num, key="page_num_input")
    if page_input != st.session_state.page_num:
        st.session_state.page_num = page_input
        st.rerun()

start_idx = (st.session_state.page_num - 1) * ROWS_PER_PAGE
end_idx = min(start_idx + ROWS_PER_PAGE, len(filtered_df))
page_data = filtered_df.iloc[start_idx:end_idx]

if not page_data.empty:
    st.markdown("""
    <style>
    .table-header {
        display: grid;
        grid-template-columns: 2fr 2fr 1.5fr 1.5fr 1.5fr 1.5fr 1fr;
        font-weight: bold;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .table-row {
        display: grid;
        grid-template-columns: 2fr 2fr 1.5fr 1.5fr 1.5fr 1.5fr 1fr;
        align-items: center;
        padding: 0.3rem 0;
        border-bottom: 1px solid #eee;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="table-header">
        <div>EMPI ID</div>
        <div>Timestamp</div>
        <div>Critical</div>
        <div>Incidental</div>
        <div>Score</div>
        <div>Risk Level</div>
        <div>Action</div>
    </div>
    """, unsafe_allow_html=True)

    for i, row in page_data.reset_index(drop=True).iterrows():
        with st.container():
            cols = st.columns([2, 2, 1.5, 1.5, 1.5, 1.5, 1])
            cols[0].write(row["empi_id"])
            cols[1].write(str(row["timestamp"]))
            cols[2].markdown("🔴 Yes" if row["critical_findings"] == "Yes" else "❌ No")
            cols[3].markdown("🟠 Yes" if row["incidental_findings"] == "Yes" else "❌ No")
            cols[4].write(row["mammogram_score"])
            cols[5].markdown(risk_badge(row["risk_level"]), unsafe_allow_html=True)
            with cols[6]:
                if st.button("View", key=f"view_{i}"):
                    st.session_state.selected_patient = row["empi_id"]
                    st.session_state.selected_timestamp = row["timestamp"]
                    st.switch_page("pages/patient_detail.py")
else:
    st.warning("No data available.")

# --- Excel download button ---
if not filtered_df.empty:
    excel_buffer = io.BytesIO()
    filtered_df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    st.download_button(
        label="⬇️ Download Full Table as Excel",
        data=excel_buffer,
        file_name="patient_reports.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
