import os
from datetime import datetime
import pytz
from snowflake.connector import connect

# Get Snowflake credentials from environment variables
user = os.environ.get('SNOWFLAKE_USER')
password = os.environ.get('SNOWFLAKE_PASSWORD')
account = os.environ.get('SNOWFLAKE_ACCOUNT')
warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
database = os.environ.get('SNOWFLAKE_DATABASE')
schema = os.environ.get('SNOWFLAKE_SCHEMA')

# Define SQL query
sql_statement = """
UPDATE ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG
SET bookmark = 'COMPLETE', STATUS = 'COMPLETE', END_TIMESTAMP = CURRENT_TIMESTAMP
WHERE job_name = 'dwh_start_etl_batch' AND business_date = (SELECT MAX(BUSINESS_DATE) FROM ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG)
AND BOOKMARK<>'COMPLETE' AND STATUS<>'COMPLETE';
"""

def run_sql_query():
    """
    This function runs the provided SQL query.
    """
    # Connect to Snowflake
    ctx = connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    try:
        # Create cursor and execute SQL statement
        cur = ctx.cursor()
        cur.execute(sql_statement)
        print(f"Successfully updated DWH_C_BATCH_LOG table at {datetime.now(pytz.timezone('Asia/Kathmandu'))}")
        cur.close()
    finally:
        # Close connection
        ctx.close()

# Run the SQL query
run_sql_query()
