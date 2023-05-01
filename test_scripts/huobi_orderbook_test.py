from datetime import datetime, timedelta
from time import sleep

import requests

from orderbook.HuobiOrderbook import HuobiOrderbook

ticker = "NANOUSDT"
orderbook = HuobiOrderbook(ticker, depth=5)
interval = 3

# while now < start + timedelta(minutes=5):
while True:
    while True:
        try:
            orderbook.update()
            break
        except requests.exceptions.ReadTimeout:
            print("Connection error...")
            sleep(2.0)

    print("Orderbook of", ticker, "at", str(datetime.utcnow()), "is", str(orderbook))
    sleep(interval)
    now = datetime.now()
