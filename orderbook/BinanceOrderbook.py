from collections import OrderedDict
from time import sleep

import requests
from binance import Client

from orderbook.Orderbook import Orderbook


class BinanceOrderbook(Orderbook):
    def __init__(self, ticker: str, depth=1):
        super().__init__()
        self.ticker = ticker
        self.client = Client(api_key="", api_secret="")
        self.depth = depth

    def update(self):
        while True:
            try:
                orderbook = self.client.get_order_book(symbol=self.ticker, limit=self.depth)
                self.orderbook["bids"] = OrderedDict()
                self.orderbook["asks"] = OrderedDict()
                
                for i in range(len(orderbook["bids"])):
                    self.orderbook["bids"][float(orderbook["bids"][i][0])] = float(orderbook["bids"][i][1])
                    
                for i in range(len(orderbook["asks"])):
                    self.orderbook["asks"][float(orderbook["asks"][i][0])] = float(orderbook["asks"][i][1])
                    
                break
            except requests.exceptions.ReadTimeout:
                print("BinanceOrderbook update() Connection error...")
                sleep(1.0)
