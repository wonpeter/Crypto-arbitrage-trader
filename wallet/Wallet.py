"""
An abstract class which makes transactions.
"""

from abc import ABCMeta
from abc import abstractmethod

class Wallet(metaclass=ABCMeta):
    @abstractmethod
    def purchase(self, ticker: str, amountInPurchaseCurrency: float) -> bool:
        """
        Purschases a stock/cryptocurrency.
        :param ticker: what to purchase
        :param amount: the amount to purchase, units: ticker
        :return: success of the transaction
        """
        pass

    @abstractmethod
    def sell(self, ticker: str, amountInSellCurrency: float) -> bool:
        """
        Sells a stock/cryptocurrency.
        :param ticker: what to sell
        :param amount: the amount to sell, units: ticker
        :return: success of the transaction
        """
        pass

    @abstractmethod
    def getBalance(self, ticker="BTC") -> float:
        """
        Returns amount owned of stock/cryptocurrency.
        :param ticker: the asset
        :return: amount owned
        """
        pass

    @abstractmethod
    def deposit(self, amount: float, ticker="BTC"):
        """
        Removes currency from the wallet.
        :param amount: the amount (>= 0.0)
        :param ticker: the ticker
        """
        pass

    @abstractmethod
    def withdraw(self, amount: float, ticker="BTC"):
        """
        Removes currency from the wallet.
        :param amount: the amount (>= 0.0)
        :param ticker: the ticker
        """
        pass

    def getPortionOfBalance(self, portion: float, ticker="USDT") -> float:
        """
        Returns a certain portion of the balance.
        Precondition: 0 <= portion <= 1
        :param portion: the portion of the funds, e.g. 0.5 for 50%
        :return: the portion
        """
        return self.getBalance(ticker) * portion

    def lacksFunds(self, ticker="USDT"):
        return self.getBalance(ticker) <= 0

    @abstractmethod
    def getTransactionFee(self) -> float:
        pass

    @abstractmethod
    def getTotalValueInBaseCurrency(self) -> float:
        pass

    def getTradeFee(self) -> float:
        return 0.0

    def getWithdrawalFee(self, ticker) -> float:
        return 0.0
