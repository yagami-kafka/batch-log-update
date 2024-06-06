import os
from datetime import datetime, timedelta
import pytz
import time
from snowflake.connector import connect
from threading import Thread
from flask import Flask, render_template_string
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
    print("--------------------------------")
    logger.info(np_time)
    print(np_time)
    print("--------------------------------")
    return "Batch Log update alive"

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
    WHERE business_date = (SELECT MAX(BUSINESS_DATE) FROM ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG);
    """
    
    try:
        cur = conn.cursor()
        cur.execute(sql_query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    finally:
        cur.close()
        conn.close()
    
    table_template = """
    <html>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.css" />
    <script src="https://code.jquery.com/jquery-3.7.0.min.js" integrity="sha256-2Pmvv0kuTBOenSvLm6bvfBSSHrUJ+3A7x6P5Ebd07/g=" crossorigin="anonymous"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.js"></script>
    <style>
    .styled-table {{
        border-collapse: collapse;
        margin: 25px 0;
        font-size: 0.9em;
        font-family: sans-serif;
        min-width: 400px;
        width: -webkit-fill-available;
        box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
    }}
    .styled-table thead tr {{
        background-color: #009879;
        color: #ffffff;
        text-align: left;
    }}
    .styled-table th,
    .styled-table td {{
        padding: 12px 15px;
    }}
    .styled-table tbody tr {{
        border-bottom: 1px solid #dddddd;
    }}
    .styled-table tbody tr:nth-of-type(even) {{
        background-color: #f3f3f3;
    }}
    .styled-table tbody tr:last-of-type {{
        border-bottom: 2px solid #009879;
    }}
    .styled-table tbody tr.active-row {{
        font-weight: bold;
        color: #009879;
    }}
    </style>
    <table id="batch-log-table" class="styled-table">
        <thead>
            <tr>
                {0}
            </tr>
        </thead>
        <tbody>
            {1}
        </tbody>
    </table>
    <script type="text/javascript">
        $(document).ready(function() {{
            $('#batch-log-table').DataTable();
        }});
    </script>
    </html>
    """
    
    header_html = ''.join(f'<th>{col}</th>' for col in columns)
    rows_html = ''.join('<tr class="active-row">' + ''.join(f'<td>{cell}</td>' for cell in row) + '</tr>' for row in rows)
    
    return render_template_string(table_template.format(header_html, rows_html))

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()

def run_at_specific_time(sql_query, target_time):
    while True:
        now = datetime.now(pytz.timezone('Asia/Kathmandu'))
        print(f'Current time: {now.hour}: {now.minute}')
        print(f'Target time: {target_time.hour}: {target_time.minute}')
        if now.hour == target_time.hour and now.minute == target_time.minute:
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
                cur.execute(sql_query)
                print(f"Successfully updated DWH_C_BATCH_LOG table at {now}")
                cur.close()
            finally:
                ctx.close()
            break
        else:
            print(f"Waiting to run script. Current time: {now}")
            sleep_duration = (target_time - now).total_seconds()
            print(f'Sleep duration: {sleep_duration}')
            time.sleep(sleep_duration)

keep_alive()

nepal_tz = pytz.timezone('Asia/Kathmandu')
curr_time = datetime.now(nepal_tz)
target_time = nepal_tz.localize(datetime(curr_time.year, curr_time.month, curr_time.day, 5, 50))

if target_time < datetime.now(pytz.timezone('Asia/Kathmandu')):
    target_time += timedelta(days=1)

sql_statement = """
UPDATE ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG
SET bookmark = 'COMPLETE', STATUS = 'COMPLETE', END_TIMESTAMP = CURRENT_TIMESTAMP
WHERE job_name = 'dwh_start_etl_batch' AND business_date = (SELECT MAX(BUSINESS_DATE) FROM ROBLING_UAT_DB.DW_DWH.DWH_C_BATCH_LOG)
AND BOOKMARK<>'COMPLETE' AND STATUS<>'COMPLETE';
"""

run_at_specific_time(sql_statement, target_time)
