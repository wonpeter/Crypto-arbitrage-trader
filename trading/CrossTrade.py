import time
from typing import List

from logger.Logger import Logger
from orderbook.Orderbook import Orderbook
from wallet.Wallet import Wallet


class CrossTrade:
    def __init__(self, exchangeBoughtFrom: int, exchangeSoldTo: int, currency: str, baseCurrency: str):
        self.exchangeBoughtFrom = exchangeBoughtFrom # 0 or 1
        self.exchangeSoldTo = exchangeSoldTo # 0 or 1
        self.currency = currency
        self.baseCurrency = baseCurrency
        self.currentStep = 0
        self.amountSold = 0.0
        self.amountToTransferBack = 0.0

        # In nanoseconds:
        self.transferStartTime = 0.0
        self.tradedCurrencyTransferTime = 1000000000.0 # 1 second
        # Usually it takes 5 minutes to transfer USDT on TRC20 (TRON) but we'll say it takes 6 minutes.
        self.baseCurrencyTransferTime = 360000000000.0 # 6 minutes = 360 seconds
        # self.baseCurrencyTransferTime = 1000000000.0 # 6 minutes = 360 seconds

    def step(self, orderbooks: List[Orderbook], wallets: List[Wallet], logger: Logger) -> bool:
        """

        :param orderbooks:
        :param wallets:
        :return: if the trade finished
        """
        # The step where we buy the currency on the exchange to buy from
        if self.currentStep == 0:
            balance1 = wallets[self.exchangeBoughtFrom].getBalance(self.baseCurrency)

            if balance1 <= 0.0:
                # End the trade since it cannot occur.
                logger.writeSecondary("detailed_output", str(self) + " could not step at currentStep 0 0: " + str(balance1))
                return True

            purchasablePrice, purchasableAmount = orderbooks[self.exchangeBoughtFrom].getLowestAsk()
            sellablePrice, sellableAmount = orderbooks[self.exchangeSoldTo].getHighestBid()
            logger.writeSecondary("detailed_output", str(self) + " prices: " + str(purchasablePrice) + ", " +
                                  str(purchasableAmount) + ", " + str(sellablePrice) + ", " + str(sellableAmount))

            # See how much of the traded currency we can buy based on the orderbooks of the two currencies
            self.amountInTradedCurrency = min(purchasableAmount, sellableAmount)

            # See if our wallet from the exchange we purchase from limits how much we can buy
            amountPurchasable = balance1 / purchasablePrice
            self.amountInTradedCurrency = min(self.amountInTradedCurrency, amountPurchasable)
            logger.writeSecondary("detailed_output", str(self) + " amountInTradedCurrency: " +
                                  str(self.amountInTradedCurrency) + ", " + str(amountPurchasable) + ", " +
                                  str(balance1) + ", " + str(purchasablePrice))

            if self.amountInTradedCurrency <= 0.0:
                # End the trade since it cannot occur.
                logger.writeSecondary("detailed_output", str(self) + " could not step at currentStep 0 1: " + str(self.amountInTradedCurrency))
                return True

            orderbooks[self.exchangeBoughtFrom].reduceAskQuantity(purchasablePrice, self.amountInTradedCurrency)
            self.amountInTradedCurrency *= (1.0 - wallets[self.exchangeBoughtFrom].getTradeFee())
            amountInBaseCurrency = self.amountInTradedCurrency * purchasablePrice
            wallets[self.exchangeBoughtFrom].withdraw(amountInBaseCurrency, self.baseCurrency)
            logger.writeSecondary("detailed_output", str(self) + " amountInBaseCurrency: " + str(amountInBaseCurrency) +
                                  ", " + str(self.amountInTradedCurrency))

            # We will now be transferring the currency to the other exchange, also known as "step 1"
            self.transferStartTime = time.time_ns()
            self.currentStep = 1
        elif self.currentStep == 1:
            # The traded currency is being transferred from the exchange it was bought from to the exchange it will be
            # sold to.
            if time.time_ns() < self.transferStartTime + self.tradedCurrencyTransferTime:
                # Still transferring...
                return False

            logger.writeSecondary("detailed_output", str(self) + " current step went from 1 to 2")
            self.currentStep = 2
        elif self.currentStep == 2:
            # We are ready to fulfill buy orders on the exchange we will sell to.
            # The orderbook of this exchange may have changed. We want to sell ALL of our traded currency.
            amount, baseCurrencyReceived = orderbooks[self.exchangeSoldTo].liquidate(self.amountInTradedCurrency)
            self.amountSold += amount
            transferBackAmount = baseCurrencyReceived * (1.0 - wallets[self.exchangeSoldTo].getTradeFee())\
                                         - wallets[self.exchangeSoldTo].getWithdrawalFee(self.baseCurrency)
            self.amountToTransferBack += transferBackAmount

            if self.amountSold < self.amountInTradedCurrency:
                logger.writeSecondary("detailed_output", str(self) + " could not step at currentStep 2 0: " + str(
                    self.amountInTradedCurrency) + ", " + str(self.amountSold))
                return False

            logger.writeSecondary("detailed_output", str(self) + " amount: " + str(amount) +
                                  ", " + str(baseCurrencyReceived) + ", " + str(self.amountSold) + ", " +
                                  str( self.amountToTransferBack) + ", " +
                                  str(wallets[self.exchangeSoldTo].getTradeFee()) + ", " +
                                  str(wallets[self.exchangeSoldTo].getWithdrawalFee(self.baseCurrency)) + ", " +
                                  str(transferBackAmount) + ", " + str(self.amountInTradedCurrency))
            self.currentStep = 3
            self.transferStartTime = time.time_ns()
        elif self.currentStep == 3:
            # Transfer money back to the exchange it came from
            if time.time_ns() < self.transferStartTime + self.tradedCurrencyTransferTime:
                # Still transferring...
                return False

            wallets[self.exchangeBoughtFrom].deposit(self.amountToTransferBack, self.baseCurrency)
            logger.writeSecondary("detailed_output", str(self) + " current step went from 3 to done! " +
                                  str(self.amountToTransferBack) + ", " + self.baseCurrency)
            return True

        return False

