#page/patient_detail.py
import streamlit as st
import pandas as pd
from data_retrieval import get_snowflake_data
from data_storage import load_data_sql
from utils import canonical_ts
import warnings

warnings.filterwarnings('ignore', category=UserWarning,
                        message='pandas only supports SQLAlchemy connectable')

st.set_page_config(page_title="Patient Report Detail", layout="wide")

# ——— Inline Styling ———
st.markdown("""
<style>
.report-text {
    white-space: pre-wrap;
    font-family: monospace;
    font-size: 0.9rem;
    padding: 14px;
    border-radius: 8px;
    overflow-y: auto;
    border: 3px solid rgba(100, 100, 100, 0.3);
    background-color: inherit;
    color: inherit;
    #min-height: 200px;
}
body[data-theme="dark"] .report-text {
    background-color: #1e1e1e;
    color: #f1f1f1;
    border-color: #444;
}
body[data-theme="light"] .report-text {
    background-color: #f9f9f9;
    color: #000;
    border-color: #ccc;
}
.banner {
    background-color: #006a50;
    color: white;
    padding: 1.2rem 2rem;
    border-radius: 10px;
    margin-bottom: 1.5rem;
}
</style>
""", unsafe_allow_html=True)

# ——— Check session state ———
if 'selected_patient' not in st.session_state or 'selected_timestamp' not in st.session_state:
    st.warning("No patient selected. Please go back to the dashboard.")
    if st.button("Return to Dashboard"):
        st.switch_page("streamlit_app.py")
    st.stop()

# ——— Load Data ———
patient_id = st.session_state.selected_patient
selected_timestamp = st.session_state.selected_timestamp

findings_df = load_data_sql()
# load_data_sql already converts 'timestamp' to pd.to_datetime objects.

# Convert selected_timestamp (which is a single datetime object) to its canonical string form
# canonical_ts expects a Series, so wrap selected_timestamp
if not isinstance(selected_timestamp, pd.Timestamp): # Ensure it's a pandas Timestamp for .dt accessor
    selected_timestamp = pd.Timestamp(selected_timestamp)

# Handle if selected_timestamp is NaT (though it shouldn't be if coming from session state properly)
if pd.isna(selected_timestamp):
    st.error("Selected timestamp is invalid.")
    st.stop()

canonical_selected_ts_str_series = canonical_ts(pd.Series([selected_timestamp]))

if canonical_selected_ts_str_series.empty or pd.isna(canonical_selected_ts_str_series.iloc[0]):
    st.error(f"Could not create a canonical string for selected timestamp: {selected_timestamp}")
    st.stop()
canonical_selected_ts_str = canonical_selected_ts_str_series.iloc[0]

# Create a canonical string version of the timestamp column in findings_df for comparison
findings_df['canonical_timestamp_str'] = canonical_ts(findings_df['timestamp'])

record_df = findings_df[
    (findings_df['empi_id'] == patient_id) &
    (findings_df['canonical_timestamp_str'] == canonical_selected_ts_str)
]
if record_df.empty:
    st.error("No LLM-extracted findings found for this patient/timestamp.")
    st.stop()
record = record_df.iloc[0]


@st.cache_data(show_spinner=False)
def debug_fetch_rad_rows(patient_id: str, canonical_selected_ts_str: str) -> pd.DataFrame:
    rad_query = f"""
        SELECT EMPI_ID, RADIO_REPORT_TEXT, TIMESTAMP
        FROM radio_reports
        WHERE EMPI_ID = '{patient_id}'
          AND TIMESTAMP >= TO_TIMESTAMP_NTZ('{canonical_selected_ts_str}', 'YYYY-MM-DD HH24:MI:SS')
          AND TIMESTAMP < DATEADD(SECOND, 1, TO_TIMESTAMP_NTZ('{canonical_selected_ts_str}', 'YYYY-MM-DD HH24:MI:SS'))
        LIMIT 1
    """
    df = get_snowflake_data(query=rad_query)
    
    if df is not None and not df.empty and 'TIMESTAMP' in df.columns:
        dt_series = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        if dt_series.dt.tz is not None:
            dt_series = dt_series.dt.tz_convert('UTC')
        df['TIMESTAMP_naive'] = dt_series.dt.floor("s").dt.tz_localize(None)
    elif df is not None: # DataFrame exists but might be empty or missing TIMESTAMP
        df['TIMESTAMP_naive'] = pd.Series(dtype='datetime64[ns]')
    else: # df is None
        # Create an empty DataFrame with expected columns if get_snowflake_data returns None
        df = pd.DataFrame(columns=['EMPI_ID', 'RADIO_REPORT_TEXT', 'TIMESTAMP', 'TIMESTAMP_naive'])

    return df


@st.cache_data(show_spinner=False)
def debug_fetch_clin_rows(patient_id: str, selected_datetime_obj: pd.Timestamp) -> pd.DataFrame:
    selected_timestamp_iso_str = selected_datetime_obj.isoformat()
    # Snowflake's TO_TIMESTAMP_LTZ can be sensitive; ensure the format string matches the isoformat output.
    # Python's isoformat() by default is 'YYYY-MM-DDTHH:MI:SS.ffffff' or 'YYYY-MM-DDTHH:MI:SS' if microsecond is 0
    # We might need to adjust the format string for TO_TIMESTAMP_LTZ or pre-format selected_timestamp_iso_str
    # For simplicity, assuming selected_datetime_obj is naive UTC as per canonical_ts logic,
    # or Snowflake can handle standard ISO strings.
    # A safer format for Snowflake might be 'YYYY-MM-DD HH24:MI:SS.FF'
    # Let's use a specific format for Snowflake's TO_TIMESTAMP_LTZ
    formatted_ts_for_snowflake = selected_datetime_obj.strftime('%Y-%m-%d %H:%M:%S.%f')


    clin_query = f"""
        SELECT EMPI_ID, CLINICAL_REPORT_TEXT, TIMESTAMP
        FROM clinical_reports
        WHERE EMPI_ID = '{patient_id}'
        ORDER BY ABS(EXTRACT(EPOCH_SECOND FROM TIMESTAMP) - EXTRACT(EPOCH_SECOND FROM TO_TIMESTAMP_LTZ('{formatted_ts_for_snowflake}', 'YYYY-MM-DD HH24:MI:SS.FF6')))
        LIMIT 1
    """
    df = get_snowflake_data(query=clin_query)

    if df is not None and not df.empty and 'TIMESTAMP' in df.columns:
        dt_series = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        if dt_series.dt.tz is not None:
            dt_series = dt_series.dt.tz_convert('UTC')
        df['TIMESTAMP_naive'] = dt_series.dt.floor("s").dt.tz_localize(None)
    elif df is not None: # DataFrame exists but might be empty or missing TIMESTAMP
        df['TIMESTAMP_naive'] = pd.Series(dtype='datetime64[ns]')
    else: # df is None
        df = pd.DataFrame(columns=['EMPI_ID', 'CLINICAL_REPORT_TEXT', 'TIMESTAMP', 'TIMESTAMP_naive'])
        
    return df


rad_df = debug_fetch_rad_rows(patient_id, canonical_selected_ts_str)
clin_df = debug_fetch_clin_rows(patient_id, selected_timestamp) # selected_timestamp is already a pd.Timestamp
radiology_text = rad_df.iloc[0]['RADIO_REPORT_TEXT'] if not rad_df.empty else "No radiology reports found."

# ——— Header Banner ———
exam_date = selected_timestamp.strftime('%Y-%m-%d')
st.markdown(f"""
<div class="banner">
    <div style="display: flex; justify-content: space-between; flex-wrap: wrap;">
        <div style="font-size: 1.5rem; font-weight: bold;">🏥 Patient ID: {patient_id}</div>
        <div style="font-size: 1.2rem;">📅 Exam Date: {exam_date}</div>
    </div>
    <div style="margin-top: 0.5rem;">
        🔴 <b>Critical:</b> {record['critical_findings']} &nbsp;&nbsp;
        🟠 <b>Incidental:</b> {record['incidental_findings']} &nbsp;&nbsp;
        🎯 <b>BI–RADS Score:</b> {record['mammogram_score']}
    </div>
</div>
""", unsafe_allow_html=True)

# ——— Tabs Layout ———
tabs = st.tabs(["Radiology Report", "Clinical Report", "Download JSON"])

with tabs[0]:
    col1, col2 = st.columns([3, 2])

    with col1:
        st.subheader("Original Radiology Report")
        if not rad_df.empty:
            ts = rad_df.iloc[0]['TIMESTAMP_naive'].strftime(
                '%Y-%m-%d %H:%M:%S')
            st.caption(f"Report Timestamp: {ts}")
        st.markdown(
            f"<div class='report-text'>{radiology_text}</div>", unsafe_allow_html=True)

    with col2:
        st.subheader("Extracted Findings")
        st.markdown(f"""
        <div class='report-text'><b>Critical Findings:</b> {record['critical_findings']}<br>
        <b>Incidental Findings:</b> {record['incidental_findings']}<br>
        <b>Mammogram Score:</b> {record['mammogram_score']}<br>
        <b>Follow-up Required:</b> {record['follow_up']}<br>
        <b>Risk Level:</b> {record['risk_level']}
        </div>
        """, unsafe_allow_html=True)

with tabs[1]:
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("### Original Clinical Report")
        if not clin_df.empty:
            for _, row in clin_df.iterrows():
                ts = row['TIMESTAMP_naive'].strftime('%Y-%m-%d %H:%M:%S')
                st.caption(f"Report Timestamp: {ts}")
                st.markdown(
                    f"<div class='report-text'>{row['CLINICAL_REPORT_TEXT']}</div>", unsafe_allow_html=True)
        else:
            st.warning("No clinical reports found for this patient.")

    with col2:
        st.markdown("### Clinical Summary")
        st.markdown(f"""
        <div class='report-text'>{record.get('summary', 'No summary available.')}
        </div>
        """, unsafe_allow_html=True)

    # Download JSON tab
    with tabs[2]:
        st.subheader("Download Extracted Data (JSON)")
        export_dict = {
            "empi_id": record["empi_id"],
            "timestamp": str(record["timestamp"]),
            "critical_findings": record["critical_findings"],
            "incidental_findings": record["incidental_findings"],
            "mammogram_score": record["mammogram_score"],
            "follow_up": record["follow_up"],
            "risk_level": record["risk_level"],
            "summary": record.get("summary", "N/A")
        }

        st.json(export_dict, expanded=False)

        json_str = pd.Series(export_dict).to_json(indent=2)
        st.download_button(
            label="📥 Download JSON",
            data=json_str,
            file_name=f"{record['empi_id']}_{record['timestamp']}.json",
            mime="application/json"
        )
# ——— Back button ———
st.markdown("---")
if st.button("⬅ Back to Dashboard"):
    st.switch_page("streamlit_app.py")
