from typing import List

from logger.Logger import Logger
from orderbook.Orderbook import Orderbook
from trading.CrossTrade import CrossTrade
from wallet.Wallet import Wallet


class Trader:
    def __init__(self, tradedCurrency: str, baseCurrency: str):
        self.tradedCurrency = tradedCurrency
        self.baseCurrency = baseCurrency

    def step(self, orderbooks: List[Orderbook], wallets: List[Wallet], logger: Logger) -> List[CrossTrade]:
        trades = []

        while True:
            # Keep on checking orderbook 0 to see if there is an opportunity to buy there and sell at exchange 1
            lowestAsk = orderbooks[0].getLowestAsk()
            highestBid = orderbooks[1].getHighestBid()

            if not lowestAsk[0] or not highestBid[0]:
                break

            quantity = min(lowestAsk[1], highestBid[1])
            notionalTraded0 = lowestAsk[0] * (1.0 + wallets[0].getTradeFee()) * quantity \
                + lowestAsk[0] * wallets[0].getWithdrawalFee(self.tradedCurrency) \
                + wallets[0].getWithdrawalFee(self.baseCurrency)
            notionalTraded1 = highestBid[0] * (1.0 - wallets[1].getTradeFee()) * quantity

            if notionalTraded0 < notionalTraded1:
                # print("orderbooks[0].getLowestAsk()[0] < orderbooks[1].getHighestBid()[0]:", orderbooks[0].getLowestAsk(), orderbooks[1].getHighestBid())
                trade = CrossTrade(0, 1, self.tradedCurrency, self.baseCurrency)
                # Stepping this CrossTrade will take away quantity at the best ask from orderbook 0
                trade.step(orderbooks, wallets, logger)
                trades.append(trade)
            else:
                break

        while True:
            # Keep on checking orderbook 1 to see if there is an opportunity to buy there and sell at exchange 0
            highestBid = orderbooks[0].getHighestBid()
            lowestAsk = orderbooks[1].getLowestAsk()

            if not highestBid[0] or not lowestAsk[0]:
                break

            quantity = min(lowestAsk[1], highestBid[1])
            notionalTraded0 = highestBid[0] * (1.0 - wallets[0].getTradeFee()) * quantity
            notionalTraded1 = lowestAsk[0] * (1.0 + wallets[1].getTradeFee()) * quantity \
                + lowestAsk[0] * wallets[1].getWithdrawalFee(self.tradedCurrency) \
                + wallets[1].getWithdrawalFee(self.baseCurrency)
            if notionalTraded0 > notionalTraded1:
                # print("orderbooks[0].getHighestBid()[0] > orderbooks[1].getLowestAsk()[0]:", orderbooks[0].getHighestBid()[0], orderbooks[1].getLowestAsk()[0])
                trade = CrossTrade(1, 0, self.tradedCurrency, self.baseCurrency)
                # Stepping this CrossTrade will take away quantity at the best ask from orderbook 1
                trade.step(orderbooks, wallets, logger)
                trades.append(trade)
            else:
                break

        return trades
