#streamlit_app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from data_retrieval import get_snowflake_data
from text_analysis import extract_findings, configure_gemini
from data_storage import store_data_sql, load_data_sql, init_db, reset_db, retry_failed_extractions
from utils import canonical_ts
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
    gap: 1.8rem;
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
configure_gemini()

# Sidebar dev tools
if st.sidebar.button("Reset DB"):
    reset_db()
    if 'processed' in st.session_state:
        del st.session_state.processed
    # Also clear any cached data that might depend on the old DB state
    load_radiology_data.clear()
    load_clinical_data.clear()
    st.sidebar.success("Database reset. Reprocessing will occur on next interaction/refresh.")
    st.rerun() # Use rerun to ensure the app starts fresh and hits the 'processed' check

def get_radio_for_retry(empi_id, timestamp):
    return get_snowflake_data(
        query=f"""
            SELECT RADIO_REPORT_TEXT
            FROM radio_reports
            WHERE EMPI_ID = '{empi_id}'
              AND TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') = '{timestamp}'
        """
    )

def get_clinical_for_retry(empi_id, timestamp):
    df = get_snowflake_data(
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
        query="SELECT EMPI_ID, RADIO_REPORT_TEXT, TIMESTAMP FROM radio_reports"
    )

@st.cache_data
def load_clinical_data():
    return get_snowflake_data(
        query="SELECT EMPI_ID, CLINICAL_REPORT_TEXT, TIMESTAMP FROM clinical_reports"
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
    if radio_df is None or radio_df.empty:
        st.error("Failed to load radiology data from Snowflake. Please check your .env configuration and Snowflake connection.")
        st.stop()

    clinical_df = load_clinical_data()
    if clinical_df is None or clinical_df.empty:
        st.error("Failed to load clinical data from Snowflake. Please check your .env configuration and Snowflake connection.")
        st.stop()

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
# Keep 6 columns for filters, no change here based on new table display fields.
col1, col2, col3, col4, col5, col6_filter = st.columns(6) 

with col1:
    empi_ids = ["All"] + sorted(df_display['empi_id'].unique())
    selected_empi = st.selectbox("Select EMPI ID", empi_ids)

with col2:
    # Date range filter now uses 'ai_report_timestamp'
    # load_data_sql should already convert this to datetime, but an explicit check is good.
    if 'ai_report_timestamp' not in df_display.columns:
        df_display['ai_report_timestamp'] = pd.NaT 
    df_display['ai_report_timestamp'] = pd.to_datetime(df_display['ai_report_timestamp'], errors='coerce')
    valid_timestamps = df_display['ai_report_timestamp'].dropna()

    if valid_timestamps.empty:
        st.warning("⚠️ No valid AI Report timestamps available. Skipping date filter.")
        date_range = (None, None)
    else:
        try:
            min_date = valid_timestamps.min().date()
            max_date = valid_timestamps.max().date()

            # Ensure both are valid datetime.date objects
            if pd.isna(min_date) or pd.isna(max_date):
                raise ValueError("Date range includes NaT.")

            date_range = st.date_input("Date Range", (min_date, max_date), min_value=min_date, max_value=max_date)
        except Exception as e:
            st.error(f"❌ Error setting date range filter: {e}")
            date_range = (None, None)


with col3:
    selected_critical = st.selectbox("Critical Findings", ["All", "Yes", "No"])

with col4:
    selected_followup = st.selectbox("Follow-up Needed", ["All", "Yes", "No"])

with col5:
    selected_risk = st.selectbox("Risk Level", ["All", "Low", "Medium", "High"])

with col6_filter: # New filter for response time
    response_time_options = ["All", "0-30 mins", "31-60 mins", ">60 mins", "N/A"]
    selected_response_time = st.selectbox("Response Time (min)", response_time_options)

patient_search = st.text_input("Search Patient ID")

# ------------- Filtering Logic ---------------
filtered_df = df_display.copy()
# Ensure 'critical_finding_response_time_minutes' is numeric, coercing errors to NaN
if 'critical_finding_response_time_minutes' in filtered_df.columns:
    filtered_df['critical_finding_response_time_minutes'] = pd.to_numeric(filtered_df['critical_finding_response_time_minutes'], errors='coerce')

if selected_empi != "All":
    filtered_df = filtered_df[filtered_df['empi_id'] == selected_empi]

if len(date_range) == 2 and date_range[0] is not None and date_range[1] is not None:
    start_date, end_date = date_range
    # Ensure the column exists and is not all NaT before trying .dt accessor
    if 'ai_report_timestamp' in filtered_df.columns and not filtered_df['ai_report_timestamp'].isna().all():
        filtered_df = filtered_df[
            (filtered_df['ai_report_timestamp'].dt.date >= start_date) &
            (filtered_df['ai_report_timestamp'].dt.date <= end_date)
        ]

if selected_critical != "All":
    filtered_df = filtered_df[filtered_df['critical_findings'] == selected_critical]

if selected_followup != "All":
    filtered_df = filtered_df[filtered_df['follow_up'] == selected_followup]

if selected_risk != "All":
    filtered_df = filtered_df[filtered_df['risk_level'] == selected_risk]

if selected_response_time != "All" and 'critical_finding_response_time_minutes' in filtered_df.columns:
    if selected_response_time == "N/A":
        filtered_df = filtered_df[filtered_df['critical_finding_response_time_minutes'].isna()]
    elif selected_response_time == "0-30 mins":
        filtered_df = filtered_df[(filtered_df['critical_finding_response_time_minutes'] >= 0) & (filtered_df['critical_finding_response_time_minutes'] <= 30)]
    elif selected_response_time == "31-60 mins":
        filtered_df = filtered_df[(filtered_df['critical_finding_response_time_minutes'] > 30) & (filtered_df['critical_finding_response_time_minutes'] <= 60)]
    elif selected_response_time == ">60 mins":
        filtered_df = filtered_df[filtered_df['critical_finding_response_time_minutes'] > 60]

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
        /* Adjusted for 10 columns: EMPI, AI Timestamp, Scan Type, Radiologist, Critical, Incidental, Score, Risk, Resp. Time, Action */
        grid-template-columns: 1.4fr 1.5fr 1fr 1fr 0.8fr 0.8fr 0.8fr 0.8fr 1fr 0.7fr;
        font-weight: bold;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .table-row {
        display: grid;
        /* Adjusted for 10 columns */
        grid-template-columns: 1.4fr 1.5fr 1fr 1fr 0.8fr 0.8fr 0.8fr 0.8fr 1fr 0.7fr;
        align-items: center;
        padding: 0.3rem 0;
        border-bottom: 1px solid #eee;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="table-header">
        <div>EMPI ID</div>
        <div>Exam Date</div>
        <div>Scan Type</div>
        <div>Radiologist</div>
        <div>Critical</div>
        <div>Incidental</div>
        <div>Score</div>
        <div>Risk Level</div>
        <div>Response Time (min)</div>
        <div>Action</div>
    </div>
    """, unsafe_allow_html=True)

    for i, row in page_data.reset_index(drop=True).iterrows():
        with st.container():
            cols = st.columns([1.4, 1.5, 1, 1, 0.8, 0.8, 0.8, 0.8, 1, 0.7]) # Adjusted for 10 columns
            cols[0].write(row["empi_id"])
            
            # Display AI Report Timestamp as Exam Date in MM-DD-YYYY format
            dt_val = row.get("ai_report_timestamp") # Use .get for safety
            if pd.isna(dt_val):
                display_date = "N/A"
            else:
                try:
                    # It should already be a datetime object from load_data_sql
                    display_date = dt_val.strftime('%m-%d-%Y')
                except AttributeError: 
                    display_date = "Invalid Date" # Fallback
            cols[1].write(display_date)

            cols[2].write(str(row.get("scan_type", "N/A")) if pd.notna(row.get("scan_type")) and row.get("scan_type") else "N/A")
            cols[3].write(str(row.get("radiologist_name", "N/A")) if pd.notna(row.get("radiologist_name")) and row.get("radiologist_name") else "N/A")
            
            cols[4].markdown("🔴 Yes" if row["critical_findings"] == "Yes" else "❌ No")
            cols[5].markdown("🟠 Yes" if row["incidental_findings"] == "Yes" else "❌ No")
            cols[6].write(row["mammogram_score"])
            cols[7].markdown(risk_badge(row["risk_level"]), unsafe_allow_html=True)
            
            response_time_val = row.get("critical_finding_response_time_minutes")
            if pd.isna(response_time_val):
                cols[8].write("N/A")
            elif response_time_val == 0:
                cols[8].write("0")
            else:
                cols[8].write(str(int(response_time_val)))

            with cols[9]: # Action button is now in the 10th column
                if st.button("View", key=f"view_{i}"):
                    st.session_state.selected_patient = row["empi_id"]
                    # IMPORTANT: Pass the ORIGINAL timestamp for detail page fetching logic
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