import json
from collections import OrderedDict
from time import sleep

import requests

from orderbook.Orderbook import Orderbook


class KuCoinOrderbook(Orderbook):
    def __init__(self, ticker: str, depth=1):
        super().__init__()
        self.ticker = ticker
        self.depth = depth

        # KuCoin only allows us to request with a depth of 20 or 100.
        if depth > 100 or depth < 0:
            raise Exception("KuCoinOrderbook: unsupported depth")

        self.requestedDepth = "20" if depth <= 20 else "100"

    def update(self):
        while True:
            try:
                uri = "/api/v1/market/orderbook/level2_" + self.requestedDepth + "?symbol=" + self.ticker
                response = requests.get("https://api.kucoin.com" + uri)
                orderbook = response.content.decode("utf-8")
                orderbook = json.loads(orderbook)
                orderbook = orderbook["data"]

                for i in range(min(len(orderbook["bids"]), self.depth)):
                    self.orderbook["bids"][float(orderbook["bids"][i][0])] = float(orderbook["bids"][i][1])

                for i in range(min(len(orderbook["asks"]), self.depth)):
                    self.orderbook["asks"][float(orderbook["asks"][i][0])] = float(orderbook["asks"][i][1])

                break
            except Exception as e:
                print("KuCoinOrderbook update() Connection error...")
                sleep(1.0)
