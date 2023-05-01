"""
A simple wallet for testing.
"""
import json
from typing import Dict
import random

from clock.Clock import Clock
from data_obtaining.DataObtainer import DataObtainer
from wallet.Wallet import Wallet

class SimulatedWallet(Wallet):
    baseCurrencyAmount: float
    baseCurrencyName: str
    binanceFee: float
    minimumTradeAmount: float
    balances: Dict

    # def __init__(self, dataObtainer: DataObtainer, baseCurrencyName="USDT", baseCurrencyAmount=0.0, binanceFee=0.0006, minimumTradeAmount=0.0001):
    def __init__(self, dataObtainer: DataObtainer, baseCurrencyName="USDT", baseCurrencyAmount=0.0, fee=0.0,
                 minimumTradeAmount=0.0001, withdrawalFees={}):
        self.baseCurrencyAmount = baseCurrencyAmount
        self.baseCurrencyName = baseCurrencyName
        self.binanceFee = fee
        self.minimumTradeAmount = minimumTradeAmount
        self.balances = {}
        self.dataObtainer = dataObtainer
        self.withdrawalFees = withdrawalFees

    def purchase(self, ticker: str, amountInPurchaseCurrency: float, test=True, verbose=False) -> bool:
        """
        Purchases a cryptocurrency.
        :param ticker: what to purchase, e.g. BTC (not BTCUSDT)
        :param amount: the amount to purchase, units: ticker
        :param test: whether this is a test order or a real one
        :return: success of the transaction
        """
        # currentTime = Clock.getInstance().getMinuteTimestamp()
        # amountInBaseCurrency = amountInPurchaseCurrency * self.dataObtainer.obtainSingleColumnMinuteValues(ticker + self.baseCurrencyName, "Average", startTime=currentTime, endTime=currentTime)[0]
        # amountInBaseCurrency1 = amountInPurchaseCurrency * self.dataObtainer.obtainSingleColumnMinuteValues(ticker + self.baseCurrencyName, "High", startTime=currentTime, endTime=currentTime)[0]
        # amountInBaseCurrency2 = amountInPurchaseCurrency * self.dataObtainer.obtainSingleColumnMinuteValues(ticker + self.baseCurrencyName, "Low", startTime=currentTime, endTime=currentTime)[0]
        # amountInBaseCurrency = amountInBaseCurrency2 + random.random() * (amountInBaseCurrency1 - amountInBaseCurrency2)
        rate, _, _ = self.dataObtainer.getIntraMinuteKline(ticker + self.baseCurrencyName)
        amountInBaseCurrency = rate * amountInPurchaseCurrency

        if amountInBaseCurrency < self.minimumTradeAmount:
            if verbose:
                print("Tried to purchase " + ticker + " but amount is too small: " + str(amountInBaseCurrency) + " (FakeBinanceWallet).")
            return False

        if self.baseCurrencyAmount <= amountInBaseCurrency:
            if verbose:
                print("Tried to purchase " + ticker + " with more funds than available (FakeBinanceWallet).")
            return False

        if ticker in self.balances:
            self.balances[ticker] += (1 - self.binanceFee) * amountInPurchaseCurrency
        else:
            self.balances[ticker] = (1 - self.binanceFee) * amountInPurchaseCurrency

        self.baseCurrencyAmount -= amountInBaseCurrency
        # print("Current amount of funds: " + str(self.baseCurrencyAmount) + " (FakeBinanceWallet purchase)")
        return True

    def sell(self, ticker: str, amountInSellCurrency: float, test=True) -> bool:
        """
        Sells a cryptocurrency.
        :param ticker: what to sell (e.g. BTC, not BTCUSDT)
        :param amount: the amount to sell, units: ticker
        :return: success of the transaction
        """
        # currentTime = Clock.getInstance().getMinuteTimestamp()
        # amountInBaseCurrency = amountInSellCurrency * self.dataObtainer.obtainSingleColumnMinuteValues(ticker + self.baseCurrencyName, "Average", startTime=currentTime, endTime=currentTime)[0]
        # amountInBaseCurrency1 = amountInSellCurrency * self.dataObtainer.obtainSingleColumnMinuteValues(ticker + self.baseCurrencyName, "High", startTime=currentTime, endTime=currentTime)[0]
        # amountInBaseCurrency2 = amountInSellCurrency * self.dataObtainer.obtainSingleColumnMinuteValues(ticker + self.baseCurrencyName, "Low", startTime=currentTime, endTime=currentTime)[0]
        # amountInBaseCurrency = amountInBaseCurrency2 + random.random() * (amountInBaseCurrency1 - amountInBaseCurrency2)
        rate, _, _ = self.dataObtainer.getIntraMinuteKline(ticker + self.baseCurrencyName)
        amountInBaseCurrency = rate * amountInSellCurrency

        if ticker in self.balances and self.balances[ticker] >= amountInSellCurrency:
            self.balances[ticker] -= amountInSellCurrency
        else:
            # print("Tried to sell " + ticker + " but not enough is owned (FakeBinanceWallet).")
            return False

        self.baseCurrencyAmount += (1 - self.binanceFee) * amountInBaseCurrency
        self.env.logger.writeSecondary("buys_and_sells", "FakeBinanceWallet Sell: " + str(Clock.getInstance().getTimestamp()) + " " + str(rate) + " " + str(amountInBaseCurrency))
        # print("Current amount of funds: " + str(self.baseCurrencyAmount) + " (FakeBinanceWallet sell)")
        return True

    def deposit(self, amount: float, ticker="BTC"):
        """
        Removes currency from the wallet.
        :param amount: the amount (>= 0.0)
        :param ticker: the ticker
        """
        if ticker == self.baseCurrencyName:
            self.baseCurrencyAmount += amount
        else:
            self.balances[ticker] += amount

    def withdraw(self, amount: float, ticker="BTC"):
        """
        Removes currency from the wallet.
        :param amount: the amount (>= 0.0)
        :param ticker: the ticker
        """
        fee = self.getWithdrawalFee(ticker)

        if ticker == self.baseCurrencyName:
            self.baseCurrencyAmount = max(self.baseCurrencyAmount - amount - fee, 0.0)
        else:
            self.balances[ticker] = max(self.balances[ticker] - amount - fee, 0.0)

    def canPurchase(self, ticker: str, amountInPurchaseCurrency: float, verbose=False) -> bool:
        rate, _, _ = self.dataObtainer.getIntraMinuteKline(ticker + self.baseCurrencyName)
        amountInBaseCurrency = rate * amountInPurchaseCurrency

        if amountInBaseCurrency < self.minimumTradeAmount:
            if verbose:
                print("Tried to purchase " + ticker + " but amount is too small: " + str(amountInBaseCurrency) + " (FakeBinanceWallet).")
            return False

        if self.baseCurrencyAmount <= amountInBaseCurrency:
            if verbose:
                print("Tried to purchase " + ticker + " with more funds than available (FakeBinanceWallet).")
            return False

    def canSell(self, ticker: str, amountInSellCurrency: float) -> bool:
        rate, _, _ = self.dataObtainer.getIntraMinuteKline(ticker + self.baseCurrencyName)

        if ticker in self.balances and self.balances[ticker] >= amountInSellCurrency:
            return True
        else:
            return False

    def getBalance(self, ticker="USDT") -> float:
        """
        Returns amount owned of stock/cryptocurrency.
        :param ticker: the asset
        :return: amount owned, units: ticker
        """
        if ticker == self.baseCurrencyName:
            return self.baseCurrencyAmount
        else:
            if ticker in self.balances:
                return self.balances[ticker]
            else:
                print("Tried to get balance of " + ticker + ", which is not owned (FakeBinanceWallet).")
                return 0.0

    def getTransactionFee(self) -> float:
        return self.binanceFee

    def depositCurrency(self, ticker: str, amount: float):
        if ticker in self.balances:
            self.balances[ticker] += amount
        else:
            self.balances[ticker] = amount

    def getTotalValueInBaseCurrency(self) -> float:
        total = self.baseCurrencyAmount
        currentTime = Clock.getInstance().getMinuteTimestamp()

        for ticker in self.balances:
            rate = self.dataObtainer.obtainSingleColumnMinuteValues(ticker + self.baseCurrencyName, "Average", startTime=currentTime, endTime=currentTime)[0]
            total += self.balances[ticker] * rate

        return total

    def getTradeFee(self) -> float:
        return self.binanceFee

    def getWithdrawalFee(self, ticker) -> float:
        return self.withdrawalFees[ticker] if ticker in self.withdrawalFees else 0.0

    def __str__(self):
        string = "{"

        for ticker in self.balances:
            string += ticker + ": " + str(self.balances[ticker]) + " "

        string += self.baseCurrencyName + ": " + str(self.baseCurrencyAmount) + "}"
        return string
