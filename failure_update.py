import os
from datetime import datetime, timedelta
import pytz
import time
from snowflake.connector import SnowflakeConnection

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

def run_at_specific_time(sql_query, target_time):
  """
  This function runs the provided SQL query at the specified target time.
  """
  while True:
    now = datetime.now(pytz.timezone('Asia/Kathmandu'))  # Get current time in Nepal Time
    print(f'Current time: {now.hour}: {now.minute}')
    print(f'Target time: {target_time.hour}: {target_time.minute}')
    if now.hour == target_time.hour and now.minute == target_time.minute:
      # Connect to Snowflake
      ctx = SnowflakeConnection(
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
        cur.execute(sql_query,_is_internal = True)
        print(f"Successfully updated DWH_C_BATCH_LOG table at {now}")
        cur.close()
      finally:
        # Close connection
        ctx.close()
      break
    else:
      # Wait for the target time
      print(f"Waiting to run script. Current time: {now}")
      sleep_duration = (target_time - now).total_seconds()
      print(f'Sleep duration: {sleep_duration}')
      time.sleep(sleep_duration)

# Set target time for execution (6:00 AM Nepal Time)
nepal_tz = pytz.timezone('Asia/Kathmandu')
curr_time = datetime.now(nepal_tz)
target_time = nepal_tz.localize(datetime(curr_time.year, curr_time.month, curr_time.day, 5, 50))

# Check if target time has already passed for today
if target_time < datetime.now(pytz.timezone('Asia/Kathmandu')):
  # Add a day to target time if it's already passed today
  target_time += timedelta(days = 1)

# Run the script at the target time
run_at_specific_time(sql_statement, target_time)