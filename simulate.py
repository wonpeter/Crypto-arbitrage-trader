import os
import time
from datetime import datetime
from time import sleep

from algo.Trader import Trader
from logger.Logger import Logger
from orderbook.BinanceOrderbook import BinanceOrderbook
from orderbook.HuobiOrderbook import HuobiOrderbook
from orderbook.KuCoinOrderbook import KuCoinOrderbook
from wallet.SimulatedWallet import SimulatedWallet

def setupLogger():
    nowAsString = datetime.utcnow().strftime("%m-%d-%Y-%H-%M-%S")
    path = "logs/" + nowAsString + "-both-train"
    os.mkdir(path)
    import sys
    logger = Logger(path + "/training_output.txt",
        secondaryFiles={"detailed_output": path + "/detailed_output.txt",},
        saveInterval=0)
    sys.stdout = logger
    # logger.writeSecondary("price_data_1m", "timestamp,open,high,low,close,trades")
    return logger


def main():
    logger = setupLogger()

    binanceOrderbook = BinanceOrderbook("XNOUSDT")
    # huobiOrderbook = HuobiOrderbook("NANOUSDT")
    kuCoinOrderbook = KuCoinOrderbook("XNO-USDT")
    orderbooks = [binanceOrderbook, kuCoinOrderbook]

    # Wallets:
    startingBaseCurrencyAmount = 1000
    # We don't need to give our wallets DataObtainers since we aren't calling purchase() or sell()
    binanceWallet = SimulatedWallet(None, baseCurrencyName="USDT", baseCurrencyAmount=startingBaseCurrencyAmount,
                                    fee=0.001, withdrawalFees={"XNO": 0.028, "USDT": 1.0})
    # huobiWallet = SimulatedWallet(None, baseCurrencyName="USDT", baseCurrencyAmount=startingBaseCurrencyAmount,
    #                                 fee=0.002)
    kuCoinWallet = SimulatedWallet(None, baseCurrencyName="USDT", baseCurrencyAmount=startingBaseCurrencyAmount,
                                  fee=0.001, withdrawalFees={"XNO": 0.02, "USDT": 1.0})
    wallets = [binanceWallet, kuCoinWallet]

    # What decides our trades:
    trader = Trader("XNO", "USDT")

    currentTrades = []
    lastOrderbookUpdateTime = 0
    orderbookUpdateInterval = 20000000000 # 20 seconds in nanoseconds
    lastOrderbookOutputTime = 0
    orderbookOutputInterval = 60000000000 # 60 seconds in nanoseconds
    lastWalletOutputTime = 0
    walletOutputInterval = 60000000000 # 60 seconds in nanoseconds

    while True:
        now = time.time_ns()

        if now >= lastOrderbookUpdateTime + orderbookUpdateInterval:
            binanceOrderbook.update()
            # huobiOrderbook.update()
            kuCoinOrderbook.update()
            lastOrderbookUpdateTime = now

        if now >= lastOrderbookOutputTime + orderbookOutputInterval:
            # print(datetime.utcnow(), "orderbooks, Binance:", str(binanceOrderbook), "Huobi:", str(huobiOrderbook))
            print(datetime.utcnow(), "orderbooks, Binance:", str(binanceOrderbook), "KuCoin:", str(kuCoinOrderbook))
            lastOrderbookOutputTime = now

        currentTrades += trader.step(orderbooks, wallets, logger)
        toPop = []

        for i in range(len(currentTrades)):
            done = currentTrades[i].step(orderbooks, wallets, logger)

            if done:
                toPop.append(i)

        for i in range(len(toPop)):
            currentTrades.pop(toPop[i] - i)

        if now >= lastWalletOutputTime + walletOutputInterval:
            # print(datetime.utcnow(), "wallets, Binance:", wallets[0], "Huobi:", wallets[1])
            print(datetime.utcnow(), "wallets, Binance:", wallets[0], "KuCoin:", wallets[1])
            lastWalletOutputTime = now

        sleep(0.05)

if __name__ == "__main__":
    main()

