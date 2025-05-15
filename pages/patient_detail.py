import streamlit as st
import pandas as pd
from data_retrieval import get_snowflake_data
from data_storage import load_data_sql
import warnings

# Suppress SQLAlchemy warning (if needed)
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

# ——— Page config & title ———
st.set_page_config(page_title="Patient Report Detail", layout="wide")
st.title("📋 Radiology and Clinical Report Detail")

# ——— Inline CSS for the report panels ———
st.markdown("""<style>
.report-text {
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

# ——— Ensure a patient is selected ———
if 'selected_patient' not in st.session_state or 'selected_timestamp' not in st.session_state:
    st.warning("No patient selected. Please go back to the dashboard.")
    if st.button("Return to Dashboard"):
        st.switch_page("streamlit_app.py")
    st.stop()

patient_id = st.session_state.selected_patient
selected_timestamp = st.session_state.selected_timestamp  # a timezone-naive pd.Timestamp

# ——— Load our extracted findings from SQLite ———
findings_df = load_data_sql()
findings_df['timestamp'] = pd.to_datetime(findings_df['timestamp'])
record_df = findings_df[
    (findings_df['empi_id'] == patient_id) &
    (findings_df['timestamp'] == selected_timestamp)
]
if record_df.empty:
    st.error("No LLM-extracted findings found for this patient/timestamp.")
    st.stop()
record = record_df.iloc[0]

# ——— Helper: fetch up to 10 recent rows from radio_reports ———
@st.cache_data(show_spinner=False)
def debug_fetch_rad_rows(empi_id: str) -> pd.DataFrame:
    query = f"""
        SELECT EMPI_ID, REPORT_TEXT, TIMESTAMP
        FROM radio_reports
        WHERE EMPI_ID = '{empi_id}'
        ORDER BY TIMESTAMP DESC
        LIMIT 10
    """
    df = get_snowflake_data(
        user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='INFORMATION_SCHEMA',
        query=query
    )
    # Normalize timestamps for comparison by removing milliseconds and timezone info
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.floor('s')  # Use 's' instead of 'S'
    df['TIMESTAMP_naive'] = df['TIMESTAMP'].dt.tz_localize(None)
    return df

# ——— Helper: fetch up to 10 recent rows from clinical_reports ———
@st.cache_data(show_spinner=False)
def debug_fetch_clin_rows(empi_id: str) -> pd.DataFrame:
    query = f"""
        SELECT EMPI_ID, REPORT_TEXT, TIMESTAMP
        FROM clinical_reports
        WHERE EMPI_ID = '{empi_id}'
        ORDER BY TIMESTAMP DESC
        LIMIT 10
    """
    df = get_snowflake_data(
        user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419!',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='INFORMATION_SCHEMA',
        query=query
    )
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.floor('s')  # Use 's' instead of 'S'
    df['TIMESTAMP_naive'] = df['TIMESTAMP'].dt.tz_localize(None)
    return df

# ——— Match the selected timestamp for radiology ———
rad_df = debug_fetch_rad_rows(patient_id)

# Round the selected timestamp to the nearest second for a more reliable comparison
selected_timestamp_normalized = selected_timestamp.floor('s')

matched = rad_df[rad_df['TIMESTAMP_naive'] == selected_timestamp_normalized]
if not matched.empty:
    radiology_text = matched.iloc[0]['REPORT_TEXT']
else:
    radiology_text = "No report text found."

# ——— Now render two tabs ———
tabs = st.tabs(["Radiology Report", "Clinical Report"])

with tabs[0]:
    st.subheader("Original Radiology Report")
    st.markdown(f"<div class='report-text'>{radiology_text}</div>", unsafe_allow_html=True)

    st.subheader("Extracted Findings")
    st.markdown(f"""
    <div class='report-text'>
    <b>Critical Findings:</b> {record['critical_findings']}<br>
    <b>Incidental Findings:</b> {record['incidental_findings']}<br>
    <b>Mammogram Score:</b> {record['mammogram_score']}<br>
    <b>Follow-up Required:</b> {record['follow_up']}
    </div>
    """, unsafe_allow_html=True)

with tabs[1]:
    clin_df = debug_fetch_clin_rows(patient_id)
    if not clin_df.empty:
        st.markdown("### Clinical Reports")
        for _, row in clin_df.iterrows():
            ts = row['TIMESTAMP_naive'].strftime('%Y-%m-%d %H:%M:%S')
            st.subheader(f"Report Timestamp: {ts}")
            st.markdown(f"<div class='report-text'>{row['REPORT_TEXT']}</div>", unsafe_allow_html=True)
    else:
        st.warning("No clinical reports found for this patient.")
