from collections import OrderedDict
from time import sleep

import requests

from huobi.client.market import MarketClient
from huobi.constant import DepthStep
from orderbook.Orderbook import Orderbook


class HuobiOrderbook(Orderbook):
    def __init__(self, ticker: str, depth=1):
        super().__init__()
        self.ticker = ticker.lower()
        self.client = MarketClient()
        self.depth = depth

    def update(self):
        while True:
            try:
                orderbook = self.client.get_pricedepth(self.ticker, DepthStep.STEP0, self.depth)
                self.orderbook["bids"] = OrderedDict()
                self.orderbook["asks"] = OrderedDict()

                for i in range(len(orderbook.bids)):
                    self.orderbook["bids"][orderbook.bids[i].price] = orderbook.bids[i].amount

                for i in range(len(orderbook.asks)):
                    self.orderbook["asks"][orderbook.asks[i].price] = orderbook.asks[i].amount
                break
            except Exception as e:
                print("HuobiOrderbook update() Connection error...")
                sleep(1.0)
