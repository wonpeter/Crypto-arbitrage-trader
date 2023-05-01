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
from util.Datetime import roundDownToMinute


class PhemexDataObtainer(DataObtainer):
    dateOfStart: datetime
    filePathPrefix: str
    timezone: str
    binanceClient: Client
    phemexConnection: PhemexConnection
    propertiesFile: str

    _1MinDataAsDataFrames: Dict[str, pd.DataFrame]
    _1HourDataAsDataFrames: Dict[str, pd.DataFrame]
    _1DayDataAsDataFrames: Dict[str, pd.DataFrame]
    _obtained: bool
    _id: str
    _phemexAPIKey: str
    _phemexAPISecretKey: str
    _obtainedMinuteHistoricalPhemexKlines: bool
    _obtainedHourHistoricalPhemexKlines: bool
    _obtainedDayHistoricalPhemexKlines: bool
    _phemexDataLock: th.Lock

    _onNewMinute: List
    _onNewHour: List
    _onNewDay: List

    def __init__(self, dateOfStart: datetime, propertiesFile: str, logger: Logger, filePathPrefix=""):
        self.propertiesFile = propertiesFile
        self._1MinDataAsDataFrames = {}
        self._1HourDataAsDataFrames = {}
        self._1DayDataAsDataFrames = {}
        self._obtained = False
        self._id = ""
        self._phemexAPIKey = ""
        self._phemexAPISecretKey = ""
        self._tryAmount = PHEMEX_DATA_FETCH_ATTEMPT_AMOUNT
        self._obtainedMinuteHistoricalPhemexKlines = False
        self._obtainedHourHistoricalPhemexKlines = False
        self._obtainedDayHistoricalPhemexKlines = False
        self._phemexDataLock = th.Lock()

        self.filePathPrefix = filePathPrefix
        self.dateOfStart = dateOfStart
        self.binanceClient = Client(api_key="Redacted", api_secret="Redacted")

        # This sets up the connection with Phemex.
        self.phemexConnection = PhemexConnection.getInstance()
        self.phemexConnection.logger = logger
        self.phemexConnection.addMinuteUpdateListener(self._addNewMinute)
        self.phemexConnection.addHourUpdateListener(self._addNewHour)
        self.phemexConnection.addDayUpdateListener(self._addNewDay)

        self._onNewMinute = []
        self._onNewHour = []
        self._onNewDay = []

    def usePhemexKeysFromFile(self):
        try:
            with open(self.propertiesFile) as f:
                data = json.load(f)
                self._id = data["ID"]
                self._phemexAPIKey = data["API key"]
                self._phemexAPISecretKey = data["API secret"]
        except:
            print(
                "You are missing " + self.propertiesFile + ". Please ask Robert " \
                                                      "(robert.ciborowski"
                                                      "@mail.utoronto.ca) for "
                                                      "help.")

    def startUpdatingUsingPhemexAPI(self):
        # So far, we can only track one ticker with PhemexConnection.
        def func():
            self.phemexConnection.startForKlines(self.propertiesFile, list(self._1MinDataAsDataFrames.keys())[0])

        thread = th.Thread(target=func, daemon=True)
        thread.start()

    def trackTickers(self, tickerPairs: List[str], fileNamePrefix=""):
        """
        :param tickerPairs: e.g. ["BTCUSDT", "ETHUSDT"]
        """
        if self._obtained:
            return

        # downloadSpecificBinanceDataToCSV(tickerPairs, binSize="1m", fileNamePrefix=fileNamePrefix)
        # downloadSpecificBinanceDataToCSV(tickerPairs, binSize="1h", fileNamePrefix=fileNamePrefix)
        # downloadSpecificBinanceDataToCSV(tickerPairs, binSize="1d", fileNamePrefix=fileNamePrefix)

        for ticker in tickerPairs:
            with self._phemexDataLock:
                self._1MinDataAsDataFrames[ticker] = pd.DataFrame(columns=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Average"])
                self._1MinDataAsDataFrames[ticker] = self._1MinDataAsDataFrames[ticker].set_index("Timestamp")
                self._1HourDataAsDataFrames[ticker] = pd.DataFrame(columns=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Average"])
                self._1HourDataAsDataFrames[ticker] = self._1HourDataAsDataFrames[ticker].set_index("Timestamp")
                self._1DayDataAsDataFrames[ticker] = pd.DataFrame(columns=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Average"])
                self._1DayDataAsDataFrames[ticker] = self._1DayDataAsDataFrames[ticker].set_index("Timestamp")
                # self._readHistoricalTickerPairData(ticker)

        self._obtained = True

    def stopTrackingTickers(self, tickerPairs: List[str]):
        """
        :param tickerPairs: e.g. ["BTCUSDT", "ETHUSDT"]
        """
        if not self._obtained:
            return

        for ticker in tickerPairs:
            with self._phemexDataLock:
                self._1MinDataAsDataFrames.pop(ticker)
                self._1HourDataAsDataFrames.pop(ticker)
                self._1DayDataAsDataFrames.pop(ticker)

    def obtainMinuteValues(self, ticker: str, startTime=None, endTime=None, column=None, safeMode=False):
        if safeMode:
            if not self.waitForMinute(ticker, startTime):
                return None

            if not self.waitForMinute(ticker, endTime):
                return None

            with self._phemexDataLock:
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

            with self._phemexDataLock:
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

            with self._phemexDataLock:
                df = self._1DayDataAsDataFrames[ticker]
                return self._filterByTime(df, startTime, endTime, 86400, column=column)

        df = self._1DayDataAsDataFrames[ticker]
        return self._filterByTime(df, startTime, endTime, 86400, column=column)

    # This is fast
    def obtainSingleColumnMinuteValues(self, ticker: str, column, startTime=None, endTime=None, safeMode=False):
        if safeMode:
            if not self.waitForMinute(ticker, startTime):
                return None

            if not self.waitForMinute(ticker, endTime):
                return None

            with self._phemexDataLock:
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

            with self._phemexDataLock:
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

            with self._phemexDataLock:
                df = self._1DayDataAsDataFrames[ticker]
                return self._filterSingleColumnByTime(df, column, startTime, endTime, 86400)

        df = self._1DayDataAsDataFrames[ticker]
        return self._filterSingleColumnByTime(df, column, startTime, endTime, 86400)

    def addCustomMinuteColumn(self, ticker, columnName, column, updateExisting=True):
        # with self._phemexDataLock:
        if updateExisting and columnName in self._1MinDataAsDataFrames[ticker].columns:
            self._1MinDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1MinDataAsDataFrames[ticker][columnName] = column

    def addCustomHourColumn(self, ticker, columnName, column, updateExisting=True):
        # with self._phemexDataLock:
        if updateExisting and columnName in self._1HourDataAsDataFrames[ticker].columns:
            self._1HourDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1HourDataAsDataFrames[ticker][columnName] = column

    def addCustomDayColumn(self, ticker, columnName, column, updateExisting=True):
        # with self._phemexDataLock:
        if updateExisting and columnName in self._1DayDataAsDataFrames[ticker].columns:
            self._1DayDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1DayDataAsDataFrames[ticker][columnName] = column

    def updateMinuteEntry(self, ticker, columnName, timestamp, value):
        self._1MinDataAsDataFrames[ticker][columnName].loc[timestamp] = value

    def updateHourEntry(self, ticker, columnName, timestamp, value):
        self._1HourDataAsDataFrames[ticker][columnName].loc[timestamp] = value

    def updateDayEntry(self, ticker, columnName, timestamp, value):
        self._1DayDataAsDataFrames[ticker][columnName].loc[timestamp] = value

    def addEmptyMinuteColumn(self, ticker, columnName, fillValue):
        # with self._phemexDataLock:
        self._1MinDataAsDataFrames[ticker][columnName] = fillValue

    def addEmptyHourColumn(self, ticker, columnName, fillValue):
        # with self._phemexDataLock:
        self._1HourDataAsDataFrames[ticker][columnName] = fillValue

    def addEmptyDayColumn(self, ticker, columnName, fillValue):
        # with self._phemexDataLock:
        self._1DayDataAsDataFrames[ticker][columnName] = fillValue

    def removeCustomMinuteColumn(self, ticker, columnName):
        # with self._phemexDataLock:
        self._1MinDataAsDataFrames[ticker].drop(columns=[columnName])

    def removeCustomHourColumn(self, ticker, columnName):
        # with self._phemexDataLock:
        self._1HourDataAsDataFrames[ticker].drop(columns=[columnName])

    def removeCustomDayColumn(self, ticker, columnName):
        # with self._phemexDataLock:
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
            # print("addRow 1", timestamp, type(data["Open"]), type(open), open, end="")
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
                    self.phemexConnection.logger.writeSecondary("price_data_1m", line)
                else:
                    # We are here when we are getting historical data at the beginning of the bot runtime.
                    for timestamp, row in data.iterrows():
                        line = str(row[columns].values).replace("\n", " ").replace("[", "")\
                            .replace("]", "")
                        line = re.sub(" +", " ", line)
                        line = str(timestamp).replace(":", "/").replace("-", "/") \
                               + "," + line.replace(" ", ",")
                        self.phemexConnection.logger.writeSecondary("price_data_1m", line)

                self._1MinDataAsDataFrames[tickerPair] = pd.concat([self._1MinDataAsDataFrames[tickerPair], data], sort=True)

            # print("addRow 2", end="")

            for func in self._onNewMinute:
                # print("onNewMinue", func, end="")
                func(timestamp)

            # print("addRow 3", end="")
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
                    self.phemexConnection.logger.writeSecondary("price_data_1h", line)
                else:
                    # We are here when we are getting historical data at the beginning of the bot runtime.
                    for timestamp, row in data.iterrows():
                        line = str(row[columns].values).replace("\n", " ").replace("[", "") \
                            .replace("]", "")
                        line = re.sub(" +", " ", line)
                        line = str(timestamp).replace(":", "/").replace("-", "/") \
                               + "," + line.replace(" ", ",")
                        self.phemexConnection.logger.writeSecondary("price_data_1h", line)

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
                    self.phemexConnection.logger.writeSecondary("price_data_1d", line)
                else:
                    # We are here when we are getting historical data at the beginning of the bot runtime.
                    for timestamp, row in data.iterrows():
                        line = str(row[columns].values).replace("\n", " ").replace("[", "") \
                            .replace("]", "")
                        line = re.sub(" +", " ", line)
                        line = str(timestamp).replace(":", "/").replace("-", "/") \
                               + "," + line.replace(" ", ",")
                        self.phemexConnection.logger.writeSecondary("price_data_1d", line)

                self._1DayDataAsDataFrames[tickerPair] = pd.concat([self._1DayDataAsDataFrames[tickerPair], data], sort=True)

            for func in self._onNewDay:
                func(timestamp)
    """
    Reads ticker data_tools into self._dataAsDataFrames
    """
    def _readHistoricalTickerPairData(self, tickerPair: str):
        with self._phemexDataLock:
            self._readTickerDataHelper(tickerPair, "1m")
            self._readTickerDataHelper(tickerPair, "1h")
            self._readTickerDataHelper(tickerPair, "1d")

    def _generateData(self, row, timing):
        open = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        dicts = []
        entries = []
        trade = float(row["trades"])

        d = {
            "Timestamp": timing,
            "Open": open,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": trade
        }

        dicts.append(d)
        entries.append([timing, open, high, low, close, trade])
        return entries, dicts

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

    def _readTickerDataHelper(self, ticker, period):
        listOfDicts = []
        entries = []

        path = self.filePathPrefix + ticker + "-" + period + "-data.csv"
        count = 0
        numSamples = 0
        previousTiming = None

        if period == "1m":
            interval = timedelta(minutes=1)
        elif period == "1h":
            interval = timedelta(hours=1)
        else:
            # 1 day
            interval = timedelta(days=1)

        try:
            with open(path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)

                for row in reader:
                    timestamp = row["timestamp"]
                    times = re.split(r'[-/:\s]\s*', timestamp)

                    if len(times) < 5:
                        continue

                    try:
                        if "/" in timestamp:
                            timing = datetime(int(times[0]), int(times[1]),
                                              int(times[2]), int(times[3]),
                                              int(times[4]))
                        else:
                            timing = datetime(int(times[2]), int(times[1]),
                                              int(times[0]), int(times[3]),
                                              int(times[4]))
                    except:
                        print("Error reading historical timestamp " + str(
                            timestamp) + " for " + ticker + ".")
                        continue

                    if timing < self.dateOfStart:
                        continue

                    if previousTiming == timing:
                        # Sometimes, Binance data has duplicate entries for some reason.
                        continue

                    if previousTiming is not None and previousTiming + interval < timing:
                        previousTiming += interval
                        while previousTiming != timing:
                            entries2, dicts = self._generateData(row, previousTiming)
                            listOfDicts += dicts
                            numSamples += 1
                            entries += entries2
                            count += 1
                            previousTiming += interval

                    previousTiming = timing
                    entries2, dicts = self._generateData(row, timing)
                    listOfDicts += dicts
                    numSamples += 1
                    entries += entries2
                    count += 1

                    if count == 10000:
                        print("Read " + ticker + " data up to " + str(timing))
                        count = 0

            df = pd.DataFrame(entries, index=[i for i in range(numSamples)],
                              columns=["Timestamp", "Open", "High", "Low",
                                       "Close", "Volume"])
            df = df.set_index("Timestamp")

            if period == "1m":
                self._1MinDataAsDataFrames[ticker] = df
            elif period == "1h":
                self._1HourDataAsDataFrames[ticker] = df
            else:
                # 1 day
                self._1DayDataAsDataFrames[ticker] = df

            self._addAverage(ticker, period)
            print("Done reading " + ticker + " historical data.")

        except IOError as e:
            print("Could not read " + path + "!")

    def _getHistoricalKlines(self, symbol, klineSize, startTime, endTime):
        klines = self.binanceClient.get_historical_klines(symbol, klineSize, startTime.strftime("%d %b %Y %H:%M:%S"),
                                                          endTime.strftime("%d %b %Y %H:%M:%S"))
        data = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av',
                                     'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
        data = data.drop(0)
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
        data.set_index('timestamp', inplace=True)
        return data

    def getCurrentPhemexPrice(self, ticker):
        for i in range(self._tryAmount):
            try:
                uri = "/md/spot/ticker/24hr?symbol=s" + ticker
                # uri = "/spot/wallets?currency=" + ticker
                seconds_since_epoch = int(time.time())
                expiry = str(seconds_since_epoch + 60)
                signature = hmac.new(bytes(self._phemexAPISecretKey, "utf-8"),
                                     msg=bytes("/md/spot/ticker/24hrsymbol=s" + ticker + expiry, "utf-8"),
                                     digestmod=hashlib.sha256).hexdigest()
                headers = {
                    "x-phemex-access-token": self._phemexAPIKey,
                    "x-phemex-request-expiry": expiry,
                    "x-phemex-request-signature": signature
                }

                response = requests.get("https://api.phemex.com" + uri, headers=headers)
                data = response.json()

                if data["error"] != None:
                    print("getBalance failed to work for " + ticker + "! Not owned currency?")
                    break

                return data["result"]["lastEp"] / 1e8
            except requests.exceptions.ReadTimeout as e:
                print(
                    "getBalance failed to work for " + ticker + "! ReadTimeout. Trying " + str(
                        self._tryAmount - 1 - i) + " more times.")
                print(e)
            except:
                print(
                    "getBalance failed to work for " + ticker + "! Unknown. Trying " + str(
                        self._tryAmount - 1 - i) + " more times.")

        return 0.0

    def getIntraMinuteKline(self, tickerPair):
        now = roundDownToMinute(datetime.utcnow())
        kline = self.phemexConnection.getOldMinuteKlineAtTimestamp(now)

        while kline is None:
            self.phemexConnection.logger.writeSecondary("data_stream", "PhemexDataObtainer: Kline was none during"
                + str(now) + " " + str(datetime.utcnow()) + ", trying -1")
            now -= timedelta(minutes=1)
            kline = self.phemexConnection.getOldMinuteKlineAtTimestamp(now)

        return kline["Close"], kline["High"], kline["Low"]


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

        with self._phemexDataLock:
            while curr <= newest:
                # print("!!! (2) Adding min at curr:", curr, "/", newest, end="")
                row = self.phemexConnection.getOldMinuteKlineAtTimestamp(curr)
                # print("... (3) ...", end="")
                if row is not None:
                    try:
                        # print("Adding row:", row["Open"], row["High"], row["Low"], row["Close"], row["Volume"])
                        self.addRow(ticker, curr, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], period="1m")
                    except Exception as e:
                        print("PhemexDataObtainer _addNewMinute error:", e)
                        return
                elif curr not in self._1MinDataAsDataFrames[ticker]:
                    # Copy the previous row.
                    # print("1 min data frame BEFORE:", self._1MinDataAsDataFrames[ticker].head())
                    self._1MinDataAsDataFrames[ticker].loc[curr] = self._1MinDataAsDataFrames[ticker].iloc[-1]
                    # import traceback
                    # print("1 min data frame AFTER:", self._1MinDataAsDataFrames[ticker].head(), self._obtainedMinuteHistoricalPhemexKlines)
                    # print("1 min data frame index:", self._1MinDataAsDataFrames[ticker].index, self._obtainedMinuteHistoricalPhemexKlines)
                    # traceback.print_stack()

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

        with self._phemexDataLock:
            while curr <= newest:
                # print("Adding hour at curr:", curr, "/", newest)
                row = self.phemexConnection.getOldHourKlineAtTimestamp(curr)

                if row is not None:
                    try:
                        self.addRow(ticker, curr, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], period="1h")
                    except Exception as e:
                        print("PhemexDataObtainer _addNewHour error:", e)
                        return
                elif curr not in self._1HourDataAsDataFrames[ticker]:
                    # Copy the previous row.
                    self._1HourDataAsDataFrames[ticker].loc[curr] = self._1HourDataAsDataFrames[ticker].iloc[-1]

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

        with self._phemexDataLock:
            while curr <= newest:
                # print("Starting add day at curr:", curr, "/", newest)
                row = self.phemexConnection.getOldDayKlineAtTimestamp(curr)

                if row is not None:
                    try:
                        self.addRow(ticker, curr, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], period="1d")
                    except Exception as e:
                        print("PhemexDataObtainer _addNewDay error:", e)
                        return
                elif curr not in self._1DayDataAsDataFrames[ticker]:
                    # Copy the previous row.
                    self._1DayDataAsDataFrames[ticker].loc[curr] = self._1DayDataAsDataFrames[ticker].iloc[-1]

                curr += timedelta(days=1)

        self._obtainedDayHistoricalPhemexKlines = True

    def getLastMinute(self, ticker):
        with self._phemexDataLock:
            return self._1MinDataAsDataFrames[ticker].index[-1]

    def getLastHour(self, ticker):
        with self._phemexDataLock:
            return self._1HourDataAsDataFrames[ticker].index[-1]

    def getLastDay(self, ticker):
        with self._phemexDataLock:
            return self._1DayDataAsDataFrames[ticker].index[-1]

    def waitForMinute(self, ticker, dt, allowAnyWaitTime=False):
        if dt is None:
            return True

        last = self.getLastMinute(ticker)
        # print(dt, type(dt), last, type(last), "are things")

        if dt > last:
            if not allowAnyWaitTime and dt > last + timedelta(minutes=2):
                print("PhemexDataObtainer: you should not request a minute so far into the future. You requested", dt, "and last is", last)
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
                print("PhemexDataObtainer: you should not request an hour so far into the future. You requested", dt, "and last is", last)
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
                print("PhemexDataObtainer: you should not request a day so far into the future. You requested", dt, "and last is", last)
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
