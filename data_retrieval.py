#data_retrieval.py

import snowflake.connector
import pandas as pd
import streamlit as st  # NEW: Use Streamlit to access secrets

def get_snowflake_data(query):
    """
    Connects to Snowflake and retrieves data using Streamlit secrets.
    """
    user = st.secrets["SNOWFLAKE_USER"]
    password = st.secrets["SNOWFLAKE_PASSWORD"]
    account = st.secrets["SNOWFLAKE_ACCOUNT"]
    warehouse = st.secrets["SNOWFLAKE_WAREHOUSE"]
    database = st.secrets["SNOWFLAKE_DATABASE"]
    schema = st.secrets["SNOWFLAKE_SCHEMA"]

    try:
        # Establish connection
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        # Fetch data
        df = pd.read_sql(query, conn)
        conn.close()

        return df
    except Exception as e:
        st.error(f"Error retrieving data from Snowflake: {e}")
        raise e

