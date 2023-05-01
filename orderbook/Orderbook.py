from collections import OrderedDict

from typing import Tuple


class Orderbook:
    def __init__(self):
        self.orderbook = {"bids": OrderedDict(), "asks": OrderedDict()}

    def update(self):
        pass

    def getHighestBid(self):
        if len(self.orderbook["bids"]) == 0:
            return None, None

        first = next(iter(self.orderbook["bids"]))
        return first, self.orderbook["bids"][first]

    def getLowestAsk(self):
        if len(self.orderbook["asks"]) == 0:
            return None, None

        first = next(iter(self.orderbook["asks"]))
        return first, self.orderbook["asks"][first]

    def reduceBidQuantity(self, price, quantity) -> float:
        """
        Reduces a bid by a certain quantity
        :param price:
        :param quantity:
        :return: amount reduced by
        """
        old = self.orderbook["bids"][price]
        newQuantity = max(old - quantity, 0.0)

        if newQuantity <= 0.0:
            self.orderbook["bids"].pop(price)
        else:
            self.orderbook["bids"][price] = newQuantity
            
        return old - newQuantity

    def reduceAskQuantity(self, price, quantity):
        """
        Reduces an ask by a certain quantity
        :param price:
        :param quantity:
        :return: amount reduced by
        """
        old = self.orderbook["asks"][price]
        newQuantity = max(old - quantity, 0.0)

        if newQuantity <= 0.0:
            self.orderbook["asks"].pop(price)
        else:
            self.orderbook["asks"][price] = newQuantity
            
        return old - newQuantity

    def liquidate(self, quantity: float) -> Tuple[float, float]:
        """
        Sell as much of the traded currency as possible.
        :param quantity: max amount to sell
        :return: amount sold, base currency received
        """
        amountSold = 0.0
        baseCurrencyReceived = 0.0

        while amountSold < quantity and len(self.orderbook["bids"]) > 0:
            price = next(iter(self.orderbook["bids"]))
            amount = self.reduceBidQuantity(price, quantity - amountSold)

            if amount == 0.0:
                return amountSold, baseCurrencyReceived

            amountSold += amount
            baseCurrencyReceived += amount * price

        return amountSold, baseCurrencyReceived


    def __str__(self):
        bids = ["(" + str(x) + ", " + str(self.orderbook["bids"][x]) + ")" for x in self.orderbook["bids"]]
        asks = ["(" + str(x) + ", " + str(self.orderbook["asks"][x]) + ")" for x in self.orderbook["asks"]]
        return "Bids: " + " ".join(bids) + " Asks: " + " ".join(asks)
