from flask import Flask
from datetime import datetime
from threading import Thread
import pytz
import random
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

app = Flask('')


@app.route('/')
def home():
    tz = pytz.timezone('Asia/Kathmandu')
    np_time = datetime.now(tz)
    print("--------------------------------")
    logger.info(np_time)
    print(np_time)
    print("--------------------------------")
    return "Check in checkout alive"


def run():
    app.run(host='0.0.0.0', port=8080)


def keep_alive():
    t = Thread(target=run)
    t.start()