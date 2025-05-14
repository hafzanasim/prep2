# streamlit_app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from data_retrieval import get_snowflake_data
from text_analysis import extract_findings, configure_gemini
from data_storage import store_data_sql, load_data_sql, init_db, reset_db
import sqlite3
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

st.set_page_config(
    page_title="Radiology Findings Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom CSS
def add_custom_css():
    st.markdown("""<style>
    .highlight { background-color: #ffeb3b; padding: 2px 0px; }
    .status-card { padding: 10px; border-radius: 5px; text-align: center; color: white; margin-bottom: 15px; }
    .critical { background-color: #dc3545; }
    .incidental { background-color: #fd7e14; }
    .followup { background-color: #ffc107; color: black; }
    .not-needed { background-color: #28a745; }
    .report-text { white-space: pre-wrap; font-family: monospace; font-size: 0.9rem; padding: 10px; background-color: #f9f9f9; border-radius: 5px; height: 350px; overflow-y: auto; }
    </style>""", unsafe_allow_html=True)

add_custom_css()

# Ensure DB initialized
init_db()

configure_gemini(api_key="AIzaSyAZ5BSEUTGEOrKeX2AIUdD-CIDuH5lTB1U")

# Sidebar dev tools
if st.sidebar.button("Reset DB"):
    reset_db()
    st.sidebar.success("Database reset. Refresh to reprocess reports.")
    st.stop()

@st.cache_data
def load_initial_data():
    return get_snowflake_data(
        user='HAFZANASIM', password='=Goodluck1234567!', account='YYB34419',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='INFORMATION_SCHEMA',
        query="SELECT EMPI_ID, REPORT_TEXT, TIMESTAMP FROM radio_reports"
    ) 

# Re-run failed LLM findings (null values)
def retry_failed_extractions(db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    df_failed = pd.read_sql_query("""
        SELECT id, empi_id, timestamp FROM findings
        WHERE critical_findings IS NULL
    """, conn)

    updated_count = 0
    for _, row in df_failed.iterrows():
        raw_df = get_snowflake_data(
            user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419',
            warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='INFORMATION_SCHEMA',
            query=f"""
            SELECT REPORT_TEXT FROM radio_reports
            WHERE EMPI_ID = '{row['empi_id']}' AND TIMESTAMP = '{row['timestamp']}'
            """
        )
        if not raw_df.empty:
            report_text = raw_df.iloc[0]['REPORT_TEXT']
            findings = extract_findings(report_text)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE findings SET
                    critical_findings = ?,
                    incidental_findings = ?,
                    mammogram_score = ?,
                    follow_up = ?
                WHERE id = ?
            """, (
                findings['critical_findings'],
                findings['incidental_findings'],
                findings['mammogram_score'],
                findings['follow_up'],
                row['id']
            ))
            updated_count += 1
    conn.commit()
    conn.close()
    return updated_count

if st.sidebar.button("Re-run failed LLM findings"):
    updated = retry_failed_extractions()
    st.sidebar.success(f"Re-extracted findings for {updated} failed records.")
    st.rerun()

if 'processed' not in st.session_state:
    # Only process once at startup
    raw_df = load_initial_data()
    raw_df['timestamp'] = pd.to_datetime(raw_df['TIMESTAMP']).dt.tz_localize(None)
    raw_df['empi_id'] = raw_df['EMPI_ID']

    stored_df = load_data_sql()
    stored_df['timestamp'] = pd.to_datetime(stored_df['timestamp']).dt.tz_localize(None)

    if not stored_df.empty:
        merged = pd.merge(
            raw_df[['empi_id', 'timestamp', 'REPORT_TEXT']],
            stored_df[['empi_id', 'timestamp']],
            on=['empi_id', 'timestamp'],
            how='left',
            indicator=True
        )
        new_reports = merged[merged['_merge'] == 'left_only']
    else:
        new_reports = raw_df.copy()

    if not new_reports.empty:
        extracted = []
        for _, row in new_reports.iterrows():
            findings = extract_findings(row['REPORT_TEXT'])
            extracted.append({
                'empi_id': row['empi_id'],
                'timestamp': row['timestamp'],
                **findings
            })
        store_data_sql(extracted)
        st.success(f"Extracted and stored {len(extracted)} new findings.")

    st.session_state.processed = True

# Load final data for dashboard
df_display = load_data_sql()

# ---------------- Filters ----------------
st.markdown("### Filters")
col1, col2, col3, col4 = st.columns(4)

with col1:
    empi_ids = ["All"] + sorted(df_display['empi_id'].unique())
    selected_empi = st.selectbox("Select EMPI ID", empi_ids)

with col2:
    df_display['timestamp'] = pd.to_datetime(df_display['timestamp'])
    min_date = df_display['timestamp'].min().date()
    max_date = df_display['timestamp'].max().date()
    date_range = st.date_input("Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

with col3:
    selected_critical = st.selectbox("Critical Findings", ["All", "Yes", "No"])

with col4:
    selected_followup = st.selectbox("Follow-up Needed", ["All", "Yes", "No"])

patient_search = st.text_input("Search Patient ID")

# ---------------- Filtering ----------------
filtered_df = df_display.copy()
if selected_empi != "All":
    filtered_df = filtered_df[filtered_df['empi_id'] == selected_empi]

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        (filtered_df['timestamp'].dt.date >= start_date) &
        (filtered_df['timestamp'].dt.date <= end_date)
    ]

if selected_critical != "All":
    filtered_df = filtered_df[filtered_df['critical_findings'] == selected_critical]

if selected_followup != "All":
    filtered_df = filtered_df[filtered_df['follow_up'] == selected_followup]

if patient_search:
    filtered_df = filtered_df[filtered_df['empi_id'].str.contains(patient_search, case=False)]

# ---------------- Summary Cards ----------------
st.markdown("### Findings Overview")
critical_count = (filtered_df['critical_findings'] == 'Yes').sum()
incidental_count = (filtered_df['incidental_findings'] == 'Yes').sum()
followup_count = (filtered_df['follow_up'] == 'Yes').sum()

cols = st.columns(4)
cols[0].markdown(f"""
<div class="status-card critical">
    <h3>Critical Findings</h3><h2>{critical_count}</h2>
</div>""", unsafe_allow_html=True)
cols[1].markdown(f"""
<div class="status-card incidental">
    <h3>Incidental Findings</h3><h2>{incidental_count}</h2>
</div>""", unsafe_allow_html=True)
cols[2].markdown(f"""
<div class="status-card followup">
    <h3>Follow-Up Required</h3><h2>{followup_count}</h2>
</div>""", unsafe_allow_html=True)
cols[3].markdown(f"""
<div class="status-card not-needed">
    <h3>No Follow-Up</h3><h2>{len(filtered_df) - followup_count}</h2>
</div>""", unsafe_allow_html=True)

# ---------------- Pie Chart ----------------
st.markdown("### Findings Distribution")
fig = px.pie(filtered_df, names='critical_findings', title='Critical Findings Distribution',
             color_discrete_sequence=px.colors.qualitative.Set2)
st.plotly_chart(fig, use_container_width=True)


# ---------------- Paginated Table ----------------
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

# Display full inline table manually
if not page_data.empty:
    st.markdown("""
    <style>
    .table-header {
        display: grid;
        grid-template-columns: 2fr 2fr 1.5fr 1.5fr 1.5fr 1fr;
        font-weight: bold;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .table-row {
        display: grid;
        grid-template-columns: 2fr 2fr 1.5fr 1.5fr 1.5fr 1fr;
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
  <div>Action</div>
</div>
""", unsafe_allow_html=True)


    for i, row in page_data.reset_index(drop=True).iterrows():
        with st.container():
            cols = st.columns([2, 2, 1.5, 1.5, 1.5, 1])
            cols[0].write(row["empi_id"])
            cols[1].write(str(row["timestamp"]))
            cols[2].markdown("🔴 Yes" if row["critical_findings"] == "Yes" else "✅ No")
            cols[3].markdown("🟠 Yes" if row["incidental_findings"] == "Yes" else "✅ No")
            cols[4].write(row["mammogram_score"])
            with cols[5]:
                if st.button("View", key=f"view_{i}"):
                    st.session_state.selected_patient = row["empi_id"]
                    st.session_state.selected_timestamp = row["timestamp"]
                    st.switch_page("pages/patient_detail.py")
else:
    st.warning("No data available.")
