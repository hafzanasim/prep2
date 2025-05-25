# Corrected code for connection in data_retrieval.py

import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_data(query):
    """
    Connects to Snowflake and retrieves data.
    """
    try:
        # Establish connection
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )

        # Fetch data
        df = pd.read_sql(query, conn)
        
        # Close connection
        conn.close()
        
        return df
    except Exception as e:
        print(f"Error retrieving data from Snowflake: {e}")
        return None