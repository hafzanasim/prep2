# Corrected code for connection in data_retrieval.py

import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))

print(f"DEBUG data_retrieval: SNOWFLAKE_USER='{os.getenv('SNOWFLAKE_USER')}'")
print(f"DEBUG data_retrieval: SNOWFLAKE_PASSWORD_IS_SET={'SET' if os.getenv('SNOWFLAKE_PASSWORD') else 'NOT SET'}") # Avoid printing actual password
print(f"DEBUG data_retrieval: SNOWFLAKE_ACCOUNT='{os.getenv('SNOWFLAKE_ACCOUNT')}'")
print(f"DEBUG data_retrieval: SNOWFLAKE_WAREHOUSE='{os.getenv('SNOWFLAKE_WAREHOUSE')}'")
print(f"DEBUG data_retrieval: SNOWFLAKE_DATABASE='{os.getenv('SNOWFLAKE_DATABASE')}'")
print(f"DEBUG data_retrieval: SNOWFLAKE_SCHEMA='{os.getenv('SNOWFLAKE_SCHEMA')}'")

def get_snowflake_data(query):
    """
    Connects to Snowflake and retrieves data.
    """
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")

    missing_vars = []
    if not user:
        missing_vars.append("SNOWFLAKE_USER")
    if not password:
        missing_vars.append("SNOWFLAKE_PASSWORD")
    if not account:
        missing_vars.append("SNOWFLAKE_ACCOUNT")
    if not warehouse:
        missing_vars.append("SNOWFLAKE_WAREHOUSE")
    if not database:
        missing_vars.append("SNOWFLAKE_DATABASE")
    if not schema:
        missing_vars.append("SNOWFLAKE_SCHEMA")

    if missing_vars:
        raise ValueError(f"Error: The following environment variables are not set: {', '.join(missing_vars)}")

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
        
        # Close connection
        conn.close()
        
        return df
    except Exception as e:
        print(f"Error retrieving data from Snowflake: {e}")
        raise e
