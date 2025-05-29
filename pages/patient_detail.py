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
        ORDER BY ABS(EXTRACT(EPOCH_SECOND FROM TIMESTAMP) - EXTRACT(EPOCH_SECOND FROM TO_TIMESTAMP_NTZ('{canonical_selected_ts_str}', 'YYYY-MM-DD HH24:MI:SS')))
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
# Use AI Report Timestamp for Exam Date display, formatted as MM-DD-YYYY
ai_ts_val = record.get('ai_report_timestamp')
if pd.isna(ai_ts_val):
    display_exam_date = "N/A"
else:
    try:
        display_exam_date = ai_ts_val.strftime('%m-%d-%Y')
    except AttributeError: # Should be a datetime object
        display_exam_date = "Invalid Date"
# Fallback for display_exam_date if AI date is missing/invalid, using original selected_timestamp
if display_exam_date == "N/A" or display_exam_date == "Invalid Date":
    if pd.notna(selected_timestamp) and hasattr(selected_timestamp, 'strftime'):
        try:
            display_exam_date = selected_timestamp.strftime('%m-%d-%Y') # Format fallback consistently
        except AttributeError:
            pass # Keep "N/A" or "Invalid Date" if selected_timestamp also problematic


# Prepare response time display
response_time_val = record.get('critical_finding_response_time_minutes')
response_time_display = "N/A"
if pd.notna(response_time_val):
    response_time_display = f"{int(response_time_val)} mins"

# Get new fields for banner
scan_type_display = record.get('scan_type', 'N/A')
if not scan_type_display or pd.isna(scan_type_display): scan_type_display = "N/A"
radiologist_name_display = record.get('radiologist_name', 'N/A')
if not radiologist_name_display or pd.isna(radiologist_name_display): radiologist_name_display = "N/A"


st.markdown(f"""
<div class="banner">
    <div style="display: flex; justify-content: space-between; flex-wrap: wrap;">
        <div style="font-size: 1.5rem; font-weight: bold;">🏥 Patient ID: {patient_id}</div>
        <div style="font-size: 1.2rem;">📅 Exam Date: {display_exam_date}</div>
    </div>
    <div style="margin-top: 0.5rem; font-size: 0.95rem;">
        📄 <b>Scan Type:</b> {scan_type_display} &nbsp;&nbsp;
        👨‍⚕️ <b>Radiologist:</b> {radiologist_name_display} <br>
        🔴 <b>Critical:</b> {record.get('critical_findings', 'N/A')} &nbsp;&nbsp;
        🟠 <b>Incidental:</b> {record.get('incidental_findings', 'N/A')} &nbsp;&nbsp;
        🎯 <b>BI–RADS Score:</b> {record.get('mammogram_score', 'N/A')} &nbsp;&nbsp;
        ⏱️ <b>Response Time:</b> {response_time_display}
    </div>
</div>
""", unsafe_allow_html=True)

# ——— Tabs Layout ———
tabs = st.tabs(["Radiology Report", "Clinical Report", "Download JSON"])

with tabs[0]:
    col1, col2 = st.columns([3, 2])

    with col1:
        st.subheader("Original Radiology Report")
        
        # Display AI Processed Report Timestamp (Exam Date) as primary for this tab, formatted as MM-DD-YYYY
        ai_report_ts_val_tab = record.get('ai_report_timestamp')
        report_exam_date_display = "N/A"
        if pd.notna(ai_report_ts_val_tab) and hasattr(ai_report_ts_val_tab, 'strftime'):
            try:
                report_exam_date_display = ai_report_ts_val_tab.strftime('%m-%d-%Y')
            except AttributeError:
                 report_exam_date_display = "Invalid Date"
        st.caption(f"Exam Date: {report_exam_date_display}")

        # Optionally, display original Snowflake timestamp if different and available
        if not rad_df.empty:
            original_ts_val = rad_df.iloc[0]['TIMESTAMP_naive']
            if pd.notna(original_ts_val) and hasattr(original_ts_val, 'strftime'):
                original_ts_display_full = original_ts_val.strftime('%Y-%m-%d %H:%M:%S')
                original_ts_display_date = original_ts_val.strftime('%m-%d-%Y')
                # Show if AI date is N/A, invalid, or if original date differs from AI date
                if report_exam_date_display in ["N/A", "Invalid Date"] or original_ts_display_date != report_exam_date_display:
                    st.caption(f"Original Report Timestamp: {original_ts_display_full}")
        
        st.markdown(
            f"<div class='report-text'>{radiology_text}</div>", unsafe_allow_html=True)

    with col2:
        st.subheader("Extracted Findings Details") # Renamed for clarity
        
        crit_text = record.get('critical_findings_text', '')
        inc_text = record.get('incidental_findings_text', '')

        # Ensure "None" or empty strings are handled gracefully for display
        crit_display_text = crit_text if crit_text and crit_text.strip().lower() not in ['none', ''] else "None extracted or N/A"
        inc_display_text = inc_text if inc_text and inc_text.strip().lower() not in ['none', ''] else "None extracted or N/A"

        st.markdown(f"""
        <div class='report-text'>
        <b>Critical Findings Text:</b><br>
        {crit_display_text}<br><br>
        <b>Incidental Findings Text:</b><br>
        {inc_display_text}<br><br>
        <b>Follow-up Required:</b> {record.get('follow_up', 'N/A')}<br>
        <b>Risk Level:</b> {record.get('risk_level', 'N/A')}
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
        
        ai_ts_export = None
        if pd.notna(record.get("ai_report_timestamp")) and hasattr(record.get("ai_report_timestamp"), 'strftime'):
            ai_ts_export = record.get("ai_report_timestamp").strftime('%Y-%m-%d %H:%M:%S')

        export_dict = {
            "empi_id": record.get("empi_id", "N/A"),
            "original_snowflake_timestamp": str(record.get("timestamp")) if pd.notna(record.get("timestamp")) else None,
            "exam_date_ai": ai_ts_export, # This is already YYYY-MM-DD HH:MM:SS or None
            "scan_type": record.get('scan_type', 'N/A'),
            "radiologist_name": record.get('radiologist_name', 'N/A'),
            "critical_findings_status": record.get("critical_findings", "N/A"), # Renamed for clarity vs text
            "critical_findings_text": record.get('critical_findings_text', 'N/A'),
            "incidental_findings_status": record.get("incidental_findings", "N/A"), # Renamed for clarity vs text
            "incidental_findings_text": record.get('incidental_findings_text', 'N/A'),
            "mammogram_score": record.get("mammogram_score", "N/A"),
            "follow_up": record.get("follow_up", "N/A"),
            "risk_level": record.get("risk_level", "N/A"),
            "summary": record.get("summary", "N/A"),
            "critical_finding_response_time_minutes": response_time_display 
        }
        # Ensure N/A string fields are consistent for export
        export_keys_to_normalize = [
            'scan_type', 'radiologist_name', 'critical_findings_status', 'critical_findings_text',
            'incidental_findings_status', 'incidental_findings_text', 'mammogram_score', 
            'follow_up', 'risk_level', 'summary'
        ]
        for key in export_keys_to_normalize:
            if export_dict[key] is None or pd.isna(export_dict[key]) or export_dict[key] == '':
                export_dict[key] = "N/A"


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
