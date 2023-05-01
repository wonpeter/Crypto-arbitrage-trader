# Name: Phemex Data Obtainer
# Author: Robert Ciborowski
# Date: 14/08/2021
# Description: Keeps track of historical stock prices from the Phemex exchange.

import csv
import json
import math
from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd
import re
import pytz
import time
import hmac
import hashlib
import requests
from binance.client import Client
import threading as th

from connections.PhemexConnection import PhemexConnection
from data_obtaining.DataObtainer import DataObtainer
from logger.Logger import Logger
from util.Constants import PHEMEX_DATA_FETCH_ATTEMPT_AMOUNT
from util.Datetime import roundDownToDay, roundDownToHour, roundDownToMinute


class BinanceDataObtainer(DataObtainer):
    dateOfStart: datetime
    filePathPrefix: str
    timezone: str
    binanceClient: Client

    _1MinDataAsDataFrames: Dict[str, pd.DataFrame]
    _1HourDataAsDataFrames: Dict[str, pd.DataFrame]
    _1DayDataAsDataFrames: Dict[str, pd.DataFrame]
    _binanceDataLock: th.Lock
    _onNewMinute: List
    _onNewHour: List
    _onNewDay: List

    def __init__(self, dateOfStart: datetime, logger: Logger, filePathPrefix=""):
        self.filePathPrefix = filePathPrefix
        self.dateOfStart = dateOfStart
        self.binanceClient = Client(api_key="Redacted", api_secret="Redacted")
        self.logger = logger
        self.tickers = set()

        self._1MinDataAsDataFrames = {}
        self._1HourDataAsDataFrames = {}
        self._1DayDataAsDataFrames = {}
        self._binanceDataLock = th.Lock()
        self._onNewMinute = []
        self._onNewHour = []
        self._onNewDay = []

    def startUpdating(self):
        thread = th.Thread(target=self._updateLoop, daemon=True)
        thread.start()

    def _updateLoop(self):
        while True:
            currentTime = datetime.utcnow()

            for ticker in self.tickers:
                with self._binanceDataLock:
                    if roundDownToMinute(currentTime) >= self._1MinDataAsDataFrames[ticker].index[-1]:
                        curr = roundDownToMinute(self._1MinDataAsDataFrames[ticker].index[-1])
                        minutes = self._getKlines(ticker, "1m", curr)

                        for row in minutes:
                            self.addRow(ticker, curr, float(row[1]), float(row[2]), float(row[3]), float(row[4]),
                                        float(row[5]), period="1m")
                            curr += timedelta(minutes=1)

                    if roundDownToHour(currentTime) >= self._1HourDataAsDataFrames[ticker].index[-1]:
                        curr = roundDownToHour(self._1HourDataAsDataFrames[ticker].index[-1])
                        hours = self._getKlines(ticker, "1h", curr)

                        for row in hours:
                            self.addRow(ticker, curr, float(row[1]), float(row[2]), float(row[3]), float(row[4]),
                                        float(row[5]), period="1h")
                            curr += timedelta(hours=1)

                    if roundDownToDay(currentTime) >= self._1DayDataAsDataFrames[ticker].index[-1]:
                        curr = roundDownToDay(self._1DayDataAsDataFrames[ticker].index[-1])
                        days = self._getKlines(ticker, "1d", curr)

                        for row in days:
                            self.addRow(ticker, curr, float(row[1]), float(row[2]), float(row[3]), float(row[4]),
                                        float(row[5]), period="1d")
                            curr += timedelta(days=1)

    def trackTickers(self, tickerPairs: List[str], fileNamePrefix=""):
        """
        :param tickerPairs: e.g. ["BTCUSDT", "ETHUSDT"]
        """
        for t in tickerPairs:
            self.tickers.add(t)
            self._1MinDataAsDataFrames[t] = pd.DataFrame()
            self._1HourDataAsDataFrames[t] = pd.DataFrame()
            self._1DayDataAsDataFrames[t] = pd.DataFrame()

        # This gets historical data for our tickers.
        for ticker in tickerPairs:
            with self._binanceDataLock:
                curr = roundDownToMinute(datetime.utcnow() - timedelta(minutes=1439))
                minutes = self._getKlines(ticker, "1m", curr)

                for row in minutes:
                    self.addRow(ticker, curr, float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]), period="1m")
                    curr += timedelta(minutes=1)

                curr = roundDownToHour(datetime.utcnow() - timedelta(hours=23))
                hours = self._getKlines(ticker, "1h", curr)

                for row in hours:
                    self.addRow(ticker, curr, float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]),
                                period="1h")
                    curr += timedelta(hours=1)

                curr = roundDownToDay(datetime.utcnow() - timedelta(days=1))
                days = self._getKlines(ticker, "1d", curr)

                for row in days:
                    self.addRow(ticker, curr, float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]),
                                period="1d")
                    curr += timedelta(days=1)

    def stopTrackingTickers(self, tickerPairs: List[str]):
        """
        :param tickerPairs: e.g. ["BTCUSDT", "ETHUSDT"]
        """
        for ticker in tickerPairs:
            with self._binanceDataLock:
                self._1MinDataAsDataFrames.pop(ticker)
                self._1HourDataAsDataFrames.pop(ticker)
                self._1DayDataAsDataFrames.pop(ticker)
                self.tickers.remove(ticker)

    # This is fast
    def obtainSingleColumnMinuteValues(self, ticker: str, column, startTime=None, endTime=None, safeMode=False):
        if safeMode:
            if not self.waitForMinute(ticker, startTime):
                return None

            if not self.waitForMinute(ticker, endTime):
                return None

            with self._binanceDataLock:
                df = self._1MinDataAsDataFrames[ticker]
                return self._filterSingleColumnByTime(df, column, startTime, endTime, 60)

        df = self._1MinDataAsDataFrames[ticker]
        return self._filterSingleColumnByTime(df, column, startTime, endTime, 60)

    # This is fast
    def obtainSingleColumnHourValues(self, ticker: str, column, startTime=None, endTime=None, safeMode=False):
        if safeMode:
            if not self.waitForHour(ticker, startTime):
                return None

            if not self.waitForHour(ticker, endTime):
                return None

            with self._binanceDataLock:
                df = self._1HourDataAsDataFrames[ticker]
                return self._filterSingleColumnByTime(df, column, startTime, endTime, 3600)

        df = self._1HourDataAsDataFrames[ticker]
        return self._filterSingleColumnByTime(df, column, startTime, endTime, 3600)

    # This is fast
    def obtainSingleColumnDayValues(self, ticker: str, column, startTime=None, endTime=None, safeMode=False):
        if safeMode:
            if not self.waitForDay(ticker, startTime):
                return None

            if not self.waitForDay(ticker, endTime):
                return None

            with self._binanceDataLock:
                df = self._1DayDataAsDataFrames[ticker]
                return self._filterSingleColumnByTime(df, column, startTime, endTime, 86400)

        df = self._1DayDataAsDataFrames[ticker]
        return self._filterSingleColumnByTime(df, column, startTime, endTime, 86400)

    def addCustomMinuteColumn(self, ticker, columnName, column, updateExisting=True):
        # with self._binanceDataLock:
        if updateExisting and columnName in self._1MinDataAsDataFrames[ticker].columns:
            self._1MinDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1MinDataAsDataFrames[ticker][columnName] = column

    def addCustomHourColumn(self, ticker, columnName, column, updateExisting=True):
        # with self._binanceDataLock:
        if updateExisting and columnName in self._1HourDataAsDataFrames[ticker].columns:
            self._1HourDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1HourDataAsDataFrames[ticker][columnName] = column

    def addCustomDayColumn(self, ticker, columnName, column, updateExisting=True):
        # with self._binanceDataLock:
        if updateExisting and columnName in self._1DayDataAsDataFrames[ticker].columns:
            self._1DayDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1DayDataAsDataFrames[ticker][columnName] = column

    def updateMinuteEntry(self, ticker, columnName, timestamp, value):
        if columnName not in self._1MinDataAsDataFrames[ticker].columns:
            self._1MinDataAsDataFrames[ticker][columnName] = 0.0

        self._1MinDataAsDataFrames[ticker][columnName].loc[timestamp] = value

    def updateHourEntry(self, ticker, columnName, timestamp, value):
        if columnName not in self._1HourDataAsDataFrames[ticker].columns:
            self._1HourDataAsDataFrames[ticker][columnName] = 0.0

        self._1HourDataAsDataFrames[ticker][columnName].loc[timestamp] = value

    def updateDayEntry(self, ticker, columnName, timestamp, value):
        if columnName not in self._1DayDataAsDataFrames[ticker].columns:
            self._1DayDataAsDataFrames[ticker][columnName] = 0.0

        self._1DayDataAsDataFrames[ticker][columnName].loc[timestamp] = value

    def addEmptyMinuteColumn(self, ticker, columnName, fillValue):
        # with self._binanceDataLock:
        self._1MinDataAsDataFrames[ticker][columnName] = fillValue

    def addEmptyHourColumn(self, ticker, columnName, fillValue):
        # with self._binanceDataLock:
        self._1HourDataAsDataFrames[ticker][columnName] = fillValue

    def addEmptyDayColumn(self, ticker, columnName, fillValue):
        # with self._binanceDataLock:
        self._1DayDataAsDataFrames[ticker][columnName] = fillValue

    def removeCustomMinuteColumn(self, ticker, columnName):
        # with self._binanceDataLock:
        self._1MinDataAsDataFrames[ticker].drop(columns=[columnName])

    def removeCustomHourColumn(self, ticker, columnName):
        # with self._binanceDataLock:
        self._1HourDataAsDataFrames[ticker].drop(columns=[columnName])

    def removeCustomDayColumn(self, ticker, columnName):
        # with self._binanceDataLock:
        self._1DayDataAsDataFrames[ticker].drop(columns=[columnName])

    def runOnNewMinute(self, func):
        self._onNewMinute.append(func)

    def runOnNewHour(self, func):
        self._onNewHour.append(func)

    def runOnNewDay(self, func):
        self._onNewDay.append(func)

    def addRow(self, tickerPair, timestamp, open, high, low, close, volume, period="1m"):
        open, high, low, close, volume = self._fixNan(open, high, low, close, volume)

        average = (high + low + close) / 3
        data = [[timestamp, open, high, low, close, volume, average]]

        data = pd.DataFrame(data, index=[0], columns=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Average"])
        data = data.set_index("Timestamp")

        if period == "1m":
            if timestamp in self._1MinDataAsDataFrames[tickerPair].index:
                self._1MinDataAsDataFrames[tickerPair].update(data)
            else:
                columns = ["Open", "High", "Low", "Close", "Volume"]

                if len(self._1MinDataAsDataFrames[tickerPair]) > 0:
                    line = str(self._1MinDataAsDataFrames[tickerPair].iloc[-1][columns].values) \
                        .replace("\n", " ").replace("[", "").replace("]", "")
                    line = re.sub(" +", " ", line)
                    line = str(self._1MinDataAsDataFrames[tickerPair].index[-1]).replace(":", "/")\
                           .replace("-", "/") + "," + line.replace(" ", ",")
                    self.logger.writeSecondary("binance_price_data_1m", line)
                else:
                    # We are here when we are getting historical data at the beginning of the bot runtime.
                    for timestamp, row in data.iterrows():
                        line = str(row[columns].values).replace("\n", " ").replace("[", "")\
                            .replace("]", "")
                        line = re.sub(" +", " ", line)
                        line = str(timestamp).replace(":", "/").replace("-", "/") \
                               + "," + line.replace(" ", ",")
                        self.logger.writeSecondary("binance_price_data_1m", line)

                self._1MinDataAsDataFrames[tickerPair] = pd.concat([self._1MinDataAsDataFrames[tickerPair], data], sort=True)

            for func in self._onNewMinute:
                func(timestamp)
        elif period == "1h":
            if timestamp in self._1HourDataAsDataFrames[tickerPair].index:
                self._1HourDataAsDataFrames[tickerPair].update(data)
            else:
                columns = ["Open", "High", "Low", "Close", "Volume"]

                if len(self._1HourDataAsDataFrames[tickerPair]) > 0:
                    line = str(self._1HourDataAsDataFrames[tickerPair].iloc[-1][columns].values) \
                        .replace("\n", " ").replace("[", "").replace("]", "")
                    line = re.sub(" +", " ", line)
                    line = str(self._1HourDataAsDataFrames[tickerPair].index[-1]).replace(":", "/") \
                               .replace("-", "/") + "," + line.replace(" ", ",")
                    self.logger.writeSecondary("binance_price_data_1h", line)
                else:
                    # We are here when we are getting historical data at the beginning of the bot runtime.
                    for timestamp, row in data.iterrows():
                        line = str(row[columns].values).replace("\n", " ").replace("[", "") \
                            .replace("]", "")
                        line = re.sub(" +", " ", line)
                        line = str(timestamp).replace(":", "/").replace("-", "/") \
                               + "," + line.replace(" ", ",")
                        self.logger.writeSecondary("binance_price_data_1h", line)

                self._1HourDataAsDataFrames[tickerPair] = pd.concat([self._1HourDataAsDataFrames[tickerPair], data], sort=True)

            for func in self._onNewHour:
                func(timestamp)
        else:
            # 1 day
            if timestamp in self._1DayDataAsDataFrames[tickerPair].index:
                self._1DayDataAsDataFrames[tickerPair].update(data)
            else:
                columns = ["Open", "High", "Low", "Close", "Volume"]

                if len(self._1DayDataAsDataFrames[tickerPair]) > 0:
                    line = str(self._1DayDataAsDataFrames[tickerPair].iloc[-1][columns].values) \
                        .replace("\n", " ").replace("[", "").replace("]", "")
                    line = re.sub(" +", " ", line)
                    line = str(self._1DayDataAsDataFrames[tickerPair].index[-1]).replace(":", "/") \
                               .replace("-", "/") + "," + line.replace(" ", ",")
                    self.logger.writeSecondary("binance_price_data_1d", line)
                else:
                    # We are here when we are getting historical data at the beginning of the bot runtime.
                    for timestamp, row in data.iterrows():
                        line = str(row[columns].values).replace("\n", " ").replace("[", "") \
                            .replace("]", "")
                        line = re.sub(" +", " ", line)
                        line = str(timestamp).replace(":", "/").replace("-", "/") \
                               + "," + line.replace(" ", ",")
                        self.logger.writeSecondary("binance_price_data_1d", line)

                self._1DayDataAsDataFrames[tickerPair] = pd.concat([self._1DayDataAsDataFrames[tickerPair], data], sort=True)

            for func in self._onNewDay:
                func(timestamp)

    def _addAverage(self, ticker, period):
        if period == "1m":
            df = self._1MinDataAsDataFrames[ticker]
        elif period == "1h":
            df = self._1HourDataAsDataFrames[ticker]
        else:
            df = self._1DayDataAsDataFrames[ticker]

        df["Average"] = (df["High"] + df["Low"] + df["Close"]) / 3

    def _filterByTime(self, df: pd.DataFrame, startTime, endTime, secondsBetweenEntries: int, column=None):
        if startTime is None:
            start = 0
        else:
            start = max(0, int((startTime - df.index[0]).total_seconds() // secondsBetweenEntries))

        if endTime is None:
            end = len(df)
        else:
            end = max(0, int((endTime - df.index[0]).total_seconds() // secondsBetweenEntries)) + 1

        if column is not None:
            return df[column].iloc[start:end]

        return df.iloc[start:end]

    def _filterSingleColumnByTime(self, df: pd.DataFrame, column: str, startTime, endTime, secondsBetweenEntries: int):
        if startTime is None:
            start = 0
        else:
            start = max(0, int((startTime - df.index[0]).total_seconds() // secondsBetweenEntries))

        if endTime is None:
            end = len(df)
        else:
            end = max(0, int((endTime - df.index[0]).total_seconds() // secondsBetweenEntries)) + 1

        x = df[column].values[start:end]

        if len(x) == 0:
            print("UH OH", start, end, startTime, endTime, df.index[0])
            print(df.tail)

        return x

    def _getHistoricalKlines(self, symbol, klineSize, startTime, endTime):
        klines = self.binanceClient.get_historical_klines(symbol, klineSize, startTime.strftime("%d %b %Y %H:%M:%S"),
                                                          endTime.strftime("%d %b %Y %H:%M:%S"))
        data = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av',
                                     'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
        data = data.drop(0)
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
        data.set_index('timestamp', inplace=True)
        return data

    def getIntraMinuteKline(self, tickerPair):
        success = False
        now = time.time_ns() // 1000000

        for i in range(5):
            try:
                trades = self.binanceClient.get_aggregate_trades(symbol=tickerPair, startTime=now - 4000,
                                                                 endTime=now,
                                                                 limit=20)
                success = True
                break
            except requests.exceptions.ReadTimeout:
                print("Connection error...")
                time.sleep(2.0)

        if not success or len(trades) == 0:
            print("Failed to get current Binance price!")
            return self._1MinDataAsDataFrames[tickerPair].iloc[-1]["Close"], self._1MinDataAsDataFrames[tickerPair].iloc[-1]["High"],\
                   self._1MinDataAsDataFrames[tickerPair].iloc[-1]["Low"]

        price = 0.0
        low = float(trades[0]['p'])
        high = float(trades[0]['p'])
        # volume = 0.0

        for trade in trades:
            p = float(trade['p'])
            # q = float(trade['q'])
            price += p
            high = max(high, p)
            low = min(low, p)
            # volume += q

        price /= len(trades)
        # volume /= len(trades)

        self.logger.writeSecondary("data_stream",
                                   "PhemexConnection getOldMinuteKlineAtTimestamp: got kline at"
                                   + str(now) + ".")
        return price, high, low

    def isReadyForUse(self):
        # The object may not be ready fo ruse if it hasn't downloaded everything
        # from Phemex yet.
        if not self._obtainedMinuteHistoricalPhemexKlines or not self._obtainedHourHistoricalPhemexKlines\
                or not self._obtainedDayHistoricalPhemexKlines:
            return False

        for ticker in self._1MinDataAsDataFrames.keys():
            if self.getLastMinute(ticker) != datetime.utcnow().replace(second=0, microsecond=0)\
                and self.getLastHour(ticker) != datetime.utcnow().replace(minute=0, second=0, microsecond=0)\
                and self.getLastDay(ticker) != datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0):
                return False

        return True

    def _addNewMinute(self, ticker: str):
        # If we haven't updated our data with the 1000 historical klines that
        # the Phemex API sends us at the beginning, do that first. Otherwise,
        # just update the single minute.
        curr = self.getLastMinute(ticker) if self._obtainedMinuteHistoricalPhemexKlines else\
            self.phemexConnection.getOldMinuteKlineTimestampAtIndex(0)
        newest = self.phemexConnection.getOldMinuteKlineTimestampAtIndex(-1)
        # print("!!! (1) Starting add min at curr:", curr, "/", newest)

        with self._binanceDataLock:
            while curr <= newest:
                # print("!!! (2) Adding min at curr:", curr, "/", newest, end="")
                row = self.phemexConnection.getOldMinuteKlineAtTimestamp(curr)
                # print("... (3) ...", end="")
                try:
                    # print("Adding row:", row["Open"], row["High"], row["Low"], row["Close"], row["Volume"])
                    self.addRow(ticker, curr, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], period="1m")
                except Exception as e:
                    print("PhemexDataObtainer _addNewMinute error:", e)
                    return

                curr += timedelta(minutes=1)
                # print("... done")

        self._obtainedMinuteHistoricalPhemexKlines = True

    def _addNewHour(self, ticker):
        # If we haven't updated our data with the 1000 historical klines that
        # the Phemex API sends us at the beginning, do that first. Otherwise,
        # just update the single hour.
        curr = self.getLastHour(ticker) if self._obtainedHourHistoricalPhemexKlines else \
            self.phemexConnection.getOldHourKlineTimestampAtIndex(0)
        newest = self.phemexConnection.getOldHourKlineTimestampAtIndex(-1)
        # print("Starting add hour at curr:", curr, "/", newest)

        with self._binanceDataLock:
            while curr <= newest:
                # print("Adding hour at curr:", curr, "/", newest)
                row = self.phemexConnection.getOldHourKlineAtTimestamp(curr)
                try:
                    self.addRow(ticker, curr, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], period="1h")
                except Exception as e:
                    print("PhemexDataObtainer _addNewHour error:", e)
                    return

                curr += timedelta(hours=1)

        self._obtainedHourHistoricalPhemexKlines = True

    def _addNewDay(self, ticker):
        # If we haven't updated our data with the 1000 historical klines that
        # the Phemex API sends us at the beginning, do that first. Otherwise,
        # just update the single day.
        curr = self.getLastDay(ticker) if self._obtainedDayHistoricalPhemexKlines else \
            self.phemexConnection.getOldDayKlineTimestampAtIndex(0)
        newest = self.phemexConnection.getOldDayKlineTimestampAtIndex(-1)
        # print("Starting add day at curr:", curr, "/", newest)

        with self._binanceDataLock:
            while curr <= newest:
                # print("Starting add day at curr:", curr, "/", newest)
                row = self.phemexConnection.getOldDayKlineAtTimestamp(curr)

                try:
                    self.addRow(ticker, curr, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], period="1d")
                except Exception as e:
                    print("PhemexDataObtainer _addNewDay error:", e)
                    return

                curr += timedelta(days=1)

        self._obtainedDayHistoricalPhemexKlines = True

    def getLastMinute(self, ticker):
        with self._binanceDataLock:
            return self._1MinDataAsDataFrames[ticker].index[-1]

    def getLastHour(self, ticker):
        with self._binanceDataLock:
            return self._1HourDataAsDataFrames[ticker].index[-1]

    def getLastDay(self, ticker):
        with self._binanceDataLock:
            return self._1DayDataAsDataFrames[ticker].index[-1]

    def obtainMinuteValues(self, ticker: str, startTime=None, endTime=None, column=None, safeMode=False):
        if safeMode:
            if not self.waitForMinute(ticker, startTime):
                return None

            if not self.waitForMinute(ticker, endTime):
                return None

            with self._binanceDataLock:
                df = self._1MinDataAsDataFrames[ticker]
                return self._filterByTime(df, startTime, endTime, 60, column=column)

        df = self._1MinDataAsDataFrames[ticker]
        return self._filterByTime(df, startTime, endTime, 60, column=column)

    def obtainHourValues(self, ticker: str, startTime=None, endTime=None, column=None, safeMode=False):
        if safeMode:
            if not self.waitForHour(ticker, startTime):
                return None

            if not self.waitForHour(ticker, endTime):
                return None

            with self._binanceDataLock:
                df = self._1HourDataAsDataFrames[ticker]
                return self._filterByTime(df, startTime, endTime, 3600, column=column)

        df = self._1HourDataAsDataFrames[ticker]
        return self._filterByTime(df, startTime, endTime, 3600, column=column)

    def obtainDayValues(self, ticker: str, startTime=None, endTime=None, column=None, safeMode=False):
        if safeMode:
            if not self.waitForDay(ticker, startTime):
                return None

            if not self.waitForDay(ticker, endTime):
                return None

            with self._binanceDataLock:
                df = self._1DayDataAsDataFrames[ticker]
                return self._filterByTime(df, startTime, endTime, 86400, column=column)

        df = self._1DayDataAsDataFrames[ticker]
        return self._filterByTime(df, startTime, endTime, 86400, column=column)

    def waitForMinute(self, ticker, dt, allowAnyWaitTime=False):
        if dt is None:
            return True

        last = self.getLastMinute(ticker)
        # print(dt, type(dt), last, type(last), "are things")

        if dt > last:
            if not allowAnyWaitTime and dt > last + timedelta(minutes=2):
                print("BinanceDataObtainer: you should not request a minute so far into the future. You requested", dt, "and last is", last)
                return False
            else:
                # Wait until we have updated our data to accommodate the start time.
                # print("removeme: waiting min")
                while True:
                    last = self.getLastMinute(ticker)

                    if dt <= last:
                        # print("exited cuz", last, dt)
                        # print(self._1MinDataAsDataFrames[ticker].iloc[len(self._1MinDataAsDataFrames[ticker]) - 3:len(self._1MinDataAsDataFrames[ticker])])
                        return True

                    time.sleep(1.0)

        return True

    def waitForHour(self, ticker, dt, allowAnyWaitTime=False):
        if dt is None:
            return True

        # with self._phemexDataLock:
        last = self.getLastHour(ticker)

        if dt > last:
            if not allowAnyWaitTime and dt > last + timedelta(hours=1):
                print("BinanceDataObtainer: you should not request an hour so far into the future. You requested", dt, "and last is", last)
                return False
            else:
                # Wait until we have updated our data to accommodate the start time.
                while True:
                    last = self.getLastHour(ticker)

                    if dt <= last:
                        return True

                    time.sleep(1.0)

        return True

    def waitForDay(self, ticker, dt, allowAnyWaitTime=False):
        if dt is None:
            return True

        # with self._phemexDataLock:
        last = self.getLastDay(ticker)

        if dt > last:
            if not allowAnyWaitTime and dt > last + timedelta(days=1):
                print("BinanceDataObtainer: you should not request a day so far into the future. You requested", dt, "and last is", last)
                return False
            else:
                # Wait until we have updated our data to accommodate the start time.
                while True:
                    last = self.getLastDay(ticker)

                    if dt <= last:
                        return True

                    time.sleep(1.0)

        return True

    def _fixNan(self, open, high, low, close, volume):
        if math.isnan(volume):
            print("PhemexDataObtainer _fixNan: received a NaN volume.")
            volume = 0

        all = []

        for x in [open, high, low, close]:
            if not math.isnan(x):
                all.append(x)

        if len(all) == 4:
            return open, high, low, close, volume

        if len(all) == 0:
            all.append(0)

        if math.isnan(open):
            print("PhemexDataObtainer _fixNan: received a NaN open.")
            open = all[0]

        if math.isnan(high):
            print("PhemexDataObtainer _fixNan: received a NaN high.")
            high = max(all)

        if math.isnan(low):
            print("PhemexDataObtainer _fixNan: received a NaN low.")
            low = min(all)

        if math.isnan(close):
            print("PhemexDataObtainer _fixNan: received a NaN close.")
            close = all[0]

        return open, high, low, close, volume

    def _getKlines(self, symbol, kline_size, oldest, fileNamePrefix=""):
        newest = datetime.utcnow()

        # We loop infinitely (rather than trying 5 times and giving up) because
        # this function gets called by code that cannot progress until we get
        # our data.
        while True:
            try:
                klines = self.binanceClient.get_historical_klines(symbol, kline_size,
                    oldest.strftime("%d %b %Y %H:%M:%S"), newest.strftime("%d %b %Y %H:%M:%S"))
                break
            except:
                # print("_getKlines() connection error.")
                time.sleep(2.0)

        return klines
        # data = pd.DataFrame(klines, columns=['Timestamp', 'Open', 'High', 'Low',
        #     'Close', 'Volume', 'close_time', 'quote_av', 'trades', 'tb_base_av',
        #     'tb_quote_av', 'ignore'])
        # data = data.drop(0)
        # data = data.drop(columns=['close_time', 'quote_av', 'trades', 'tb_base_av',
        #     'tb_quote_av', 'ignore'])
        # data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms')
        # data.set_index('Timestamp', inplace=True)
        # return data
