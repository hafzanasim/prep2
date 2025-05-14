# Corrected code for connection in data_retrieval.py

import snowflake.connector
import pandas as pd

def get_snowflake_data(user, password, account, warehouse, database, schema, query):
    """
    Connects to Snowflake and retrieves data.
    """
    try:
        # Establish connection
        conn = snowflake.connector.connect(
            user="HAFZANASIM",  # User should be in quotes
            password="Goodluck1234567!",  # Password should be in quotes
            account="EFFSIWI-YYB34419",  # Account should be in quotes
            warehouse="COMPUTE_WH",  # Warehouse should be in quotes
            database="RADIOLOGYPREP",  # Database should be in quotes
            schema="INFORMATION_SCHEMA"  # Schema should be in quotes
        )

        # Fetch data
        df = pd.read_sql(query, conn)
        
        # Close connection
        conn.close()
        
        return df
    except Exception as e:
        print(f"Error retrieving data from Snowflake: {e}")
        return None
