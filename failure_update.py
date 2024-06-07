import os
from datetime import datetime, timedelta
import pytz
import time
from snowflake.connector import connect
from threading import Thread
from flask import Flask, render_template, render_template_string
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Get Snowflake credentials from environment variables
user = os.environ.get('SNOWFLAKE_USER')
password = os.environ.get('SNOWFLAKE_PASSWORD')
account = os.environ.get('SNOWFLAKE_ACCOUNT')
warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
database = os.environ.get('SNOWFLAKE_DATABASE')
schema = os.environ.get('SNOWFLAKE_SCHEMA')

app = Flask(__name__)

@app.route('/')
def home():
    tz = pytz.timezone('Asia/Kathmandu')
    np_time = datetime.now(tz)
    logger.info(np_time)
    return render_template('home.html')

@app.route('/batch-log')
def batch_log():
    conn = connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    sql_query = """
    SELECT * FROM ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG
    WHERE business_date = (SELECT MAX(BUSINESS_DATE) FROM ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG) ORDER BY STATUS DESC;
    """

    try:
        cur = conn.cursor()
        cur.execute(sql_query, _is_internal=True)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    finally:
        cur.close()
        conn.close()
    return render_template('batch_log_template.html', headers=columns, data=rows)

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()

def run_at_specific_time(sql_query, target_hour, target_minute):
    tz = pytz.timezone('Asia/Kathmandu')
    while True:
        now = datetime.now(tz)
        print(f'Current time: {now}')
        target_time = tz.localize(datetime(now.year, now.month, now.day, target_hour, target_minute))
        
        if now >= target_time:
            target_time += timedelta(days=1)
        
        sleep_duration = (target_time - now).total_seconds()
        print(f'Target time: {target_time}')
        print(f'Sleep duration: {sleep_duration}')
        
        if sleep_duration > 0:
            time.sleep(sleep_duration)
        
        ctx = connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        try:
            cur = ctx.cursor()
            cur.execute(sql_query, _is_internal=True)
            print(f"Successfully updated DWH_C_BATCH_LOG table at {now}")
            cur.close()
        finally:
            ctx.close()
        time.sleep(60)  # To ensure that the script does not run again within the same minute

keep_alive()

sql_statement = """
UPDATE ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG
SET bookmark = 'COMPLETE', STATUS = 'COMPLETE', END_TIMESTAMP = CURRENT_TIMESTAMP
WHERE job_name = 'dwh_start_etl_batch' AND business_date = (SELECT MAX(BUSINESS_DATE) FROM ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG)
AND BOOKMARK<>'COMPLETE' AND STATUS<>'COMPLETE';
"""

run_at_specific_time(sql_statement, 5, 45)
