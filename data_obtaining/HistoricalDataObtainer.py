# Name: Historic Data Obtainer
# Author: Robert Ciborowski
# Date: 13/04/2020
# Description: Keeps track of historical stock prices from a csv. Also allows
#              you to pretend that you are in the past and get "real clock" data_tools
#              as clock passes. This class was taken from my PumpBot project and
#              adapted for this project.

import csv
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
import re
import numpy as np

from clock.Clock import Clock
from data_obtaining.DataObtainer import DataObtainer

class HistoricalDataObtainer(DataObtainer):
    dateOfStart: datetime
    dateOfEnd: datetime
    filePathPrefix: str
    timezone: str

    # Pandas makes life easy but is very slow
    _1MinDataAsDataFrames: Dict[str, pd.DataFrame]
    _1HourDataAsDataFrames: Dict[str, pd.DataFrame]
    _1DayDataAsDataFrames: Dict[str, pd.DataFrame]
    _1MinIntraMinuteDataAsDataFrames: Dict[str, pd.DataFrame]
    _obtained: bool

    # For generating a random intra minute price
    _lastIntraMinuteTime: datetime
    _lastIntraMinutePrice: float
    _lastIntraMinuteHigh: float
    _lastIntraMinuteLow: float

    _intraMinuteOpenCloseSampleRatios: List
    _intraMinuteHighSampleRatios: List
    _intraMinuteLowSampleRatios: List

    def __init__(self, dateOfStart: datetime, dateOfEnd: datetime, filePathPrefix="",
                 intraMinuteDataPath="intra_minute_data/", intraMinuteBinanceDataPath="intra_minute_data/intra_minute_data_binance/"):
        self._1MinDataAsDataFrames = {}
        self._1HourDataAsDataFrames = {}
        self._1DayDataAsDataFrames = {}
        self._1MinIntraMinuteDataAsDataFrames = {}
        self._obtained = False
        self.filePathPrefix = filePathPrefix
        self.dateOfStart = dateOfStart
        self.dateOfEnd = dateOfEnd

        # For Phemex intraminute data generation
        self._lastIntraMinuteTime = None
        self._lastIntraMinutePrice = 0.0
        self._lastIntraMinuteHigh = 0.0
        self._lastIntraMinuteLow = 0.0
        self._intraMinuteOpenCloseSampleRatios = self._loadSampleData(intraMinuteDataPath + "open_close_ratios.csv")
        self._intraMinuteHighSampleRatios = self._loadSampleData(intraMinuteDataPath + "high_real_high_ratios.csv")
        self._intraMinuteLowSampleRatios = self._loadSampleData(intraMinuteDataPath + "low_real_low_ratios.csv")

        # For Binance intraminute data generation
        self._lastIntraMinuteBinanceTime = None
        self._lastIntraMinuteBinancePrice = 0.0
        self._lastIntraMinuteBinanceHigh = 0.0
        self._lastIntraMinuteBinanceLow = 0.0
        self._intraMinuteBinanceOpenCloseSampleRatios = self._loadSampleData(intraMinuteBinanceDataPath + "open_close_ratios.csv")
        self._intraMinuteBinanceHighSampleRatios = self._loadSampleData(intraMinuteBinanceDataPath + "high_real_high_ratios.csv")
        self._intraMinuteBinanceLowSampleRatios = self._loadSampleData(intraMinuteBinanceDataPath + "low_real_low_ratios.csv")

        n = 0

        for x in self._intraMinuteHighSampleRatios:
            if x >= 0.999999999:
                n += 1

        print("Num intra highs == high", n, "/", len(self._intraMinuteHighSampleRatios), n / len(self._intraMinuteHighSampleRatios))

        n = 0

        for x in self._intraMinuteLowSampleRatios:
            if x <= 1.000000001:
                n += 1

        print("Num intra lows == low", n, "/", len(self._intraMinuteLowSampleRatios), n / len(self._intraMinuteLowSampleRatios))

    def trackTickers(self, tickerPairs: List[str]):
        """
        :param tickerPairs: e.g. ["BTCUSDT", "ETHUSDT"]
        """
        if self._obtained:
            return

        for ticker in tickerPairs:
            self._readTickerPairData(ticker)

        self._obtained = True

    def stopTrackingTickers(self, tickerPairs: List[str]):
        """
        :param tickerPairs: e.g. ["BTCUSDT", "ETHUSDT"]
        """
        if not self._obtained:
            return

        for ticker in tickerPairs:
            self._1MinDataAsDataFrames.pop(ticker)
            self._1HourDataAsDataFrames.pop(ticker)
            self._1DayDataAsDataFrames.pop(ticker)

    def obtainMinuteValues(self, ticker: str, startTime=None, endTime=None, column=None):
        df = self._1MinDataAsDataFrames[ticker]
        return self._filterByTime(df, startTime, endTime, 60, column=column)

    def obtainHourValues(self, ticker: str, startTime=None, endTime=None, column=None):
        df = self._1HourDataAsDataFrames[ticker]
        return self._filterByTime(df, startTime, endTime, 3600, column=column)

    def obtainDayValues(self, ticker: str, startTime=None, endTime=None, column=None):
        df = self._1DayDataAsDataFrames[ticker]
        return self._filterByTime(df, startTime, endTime, 86400, column=column)

    # This is fast
    def obtainSingleColumnMinuteValues(self, ticker: str, column, startTime=None, endTime=None):
        df = self._1MinDataAsDataFrames[ticker]
        return self._filterSingleColumnByTime(df, column, startTime, endTime, 60)

    # This is fast
    def obtainSingleColumnHourValues(self, ticker: str, column, startTime=None, endTime=None):
        df = self._1HourDataAsDataFrames[ticker]
        return self._filterSingleColumnByTime(df, column, startTime, endTime, 3600)

    # This is fast
    def obtainSingleColumnDayValues(self, ticker: str, column, startTime=None, endTime=None):
        df = self._1DayDataAsDataFrames[ticker]
        return self._filterSingleColumnByTime(df, column, startTime, endTime, 86400)

    def addCustomMinuteColumn(self, ticker, columnName, column, updateExisting=True):
        if updateExisting and columnName in self._1MinDataAsDataFrames[ticker].columns:
            self._1MinDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1MinDataAsDataFrames[ticker][columnName] = column

    def addCustomHourColumn(self, ticker, columnName, column, updateExisting=True):
        if updateExisting and columnName in self._1HourDataAsDataFrames[ticker].columns:
            self._1HourDataAsDataFrames[ticker][columnName].update(column)
        else:
            self._1HourDataAsDataFrames[ticker][columnName] = column

    def addCustomDayColumn(self, ticker, columnName, column, updateExisting=True):
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
        self._1MinDataAsDataFrames[ticker][columnName] = fillValue

    def addEmptyHourColumn(self, ticker, columnName, fillValue):
        self._1HourDataAsDataFrames[ticker][columnName] = fillValue

    def addEmptyDayColumn(self, ticker, columnName, fillValue):
        self._1DayDataAsDataFrames[ticker][columnName] = fillValue

    def removeCustomMinuteColumn(self, ticker, columnName):
        self._1MinDataAsDataFrames[ticker].drop(columns=[columnName])

    def removeCustomHourColumn(self, ticker, columnName):
        self._1HourDataAsDataFrames[ticker].drop(columns=[columnName])

    def removeCustomDayColumn(self, ticker, columnName):
        self._1DayDataAsDataFrames[ticker].drop(columns=[columnName])

    def getIntraMinuteKline(self, tickerPair, useSampledIntraMinuteData=False):
        return self.getIntraMinuteBinanceKline(tickerPair, useSampledIntraMinuteData=useSampledIntraMinuteData)
        minute = Clock.getInstance().getMinuteTimestamp()

        if self._lastIntraMinuteTime == minute:
            return self._lastIntraMinutePrice, self._lastIntraMinuteHigh, self._lastIntraMinuteLow

        self._lastIntraMinuteTime = minute
        valuesMinutes = self.obtainMinuteValues(tickerPair, startTime=minute, endTime=minute)
        midOpenClose = (valuesMinutes["Open"].iloc[0] + valuesMinutes["Close"].iloc[0]) / 2

        self._lastIntraMinutePrice = self._intraMinuteOpenCloseSampleRatios[
            int(min(np.random.uniform(), 0.99999) * len(self._intraMinuteOpenCloseSampleRatios))] \
            * midOpenClose
        self._lastIntraMinuteHigh = self._intraMinuteHighSampleRatios[
            int(min(np.random.uniform(), 0.99999) * len(self._intraMinuteHighSampleRatios))] \
            * valuesMinutes["High"].iloc[0]
        self._lastIntraMinuteLow = self._intraMinuteLowSampleRatios[
            int(min(np.random.uniform(), 0.99999) * len(self._intraMinuteLowSampleRatios))] \
            * valuesMinutes["Low"].iloc[0]
        return self._lastIntraMinutePrice, self._lastIntraMinuteHigh, self._lastIntraMinuteLow

    def getIntraMinuteBinanceKline(self, tickerPair, useSampledIntraMinuteData=False):
        minute = Clock.getInstance().getMinuteTimestamp()

        if useSampledIntraMinuteData:
            data = self._1MinIntraMinuteDataAsDataFrames[tickerPair].loc[minute]
            return data["Close"], data["High"], data["Low"]

        if self._lastIntraMinuteBinanceTime == minute:
            return self._lastIntraMinuteBinancePrice, self._lastIntraMinuteBinanceHigh, self._lastIntraMinuteBinanceLow

        self._lastIntraMinuteBinanceTime = minute
        valuesMinutes = self.obtainMinuteValues(tickerPair, startTime=minute, endTime=minute)
        midOpenClose = (valuesMinutes["Open"].iloc[0] + valuesMinutes["Close"].iloc[0]) / 2

        self._lastIntraMinuteBinancePrice = self._intraMinuteBinanceOpenCloseSampleRatios[
            int(min(np.random.uniform(), 0.99999) * len(self._intraMinuteBinanceOpenCloseSampleRatios))] \
            * midOpenClose
        self._lastIntraMinuteBinanceHigh = self._intraMinuteBinanceHighSampleRatios[
            int(min(np.random.uniform(), 0.99999) * len(self._intraMinuteBinanceHighSampleRatios))] \
            * valuesMinutes["High"].iloc[0]
        self._lastIntraMinuteBinanceLow = self._intraMinuteBinanceLowSampleRatios[
            int(min(np.random.uniform(), 0.99999) * len(self._intraMinuteBinanceLowSampleRatios))] \
            * valuesMinutes["Low"].iloc[0]
        return self._lastIntraMinuteBinancePrice, self._lastIntraMinuteBinanceHigh, self._lastIntraMinuteBinanceLow

    def _loadSampleData(self, path):
        values = []
        file = open(path, "r")

        for line in file.readlines():
            values.append(float(line))

        file.close()
        return values

    """
    Reads ticker data_tools into self._dataAsDataFrames
    """
    def _readTickerPairData(self, tickerPair: str):
        self._readTickerDataHelper(tickerPair, "1m")
        self._readTickerDataHelper(tickerPair, "1h")
        self._readTickerDataHelper(tickerPair, "1d")
        self._readTickerDataHelper(tickerPair, "intraminute")

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
            start = int((startTime - df.index[0]).total_seconds() // secondsBetweenEntries)

        if endTime is None:
            end = len(df)
        else:
            end = int((endTime - df.index[0]).total_seconds() // secondsBetweenEntries) + 1

        if column is not None:
            return df[column].iloc[start:end]

        return df.iloc[start:end]

        # if column is not None:
        #     return df.loc[startTime:endTime, column]
        #
        # return df.loc[startTime:endTime,]

    def _filterSingleColumnByTime(self, df: pd.DataFrame, column: str, startTime, endTime, secondsBetweenEntries: int):
        if startTime is None:
            start = 0
        else:
            start = int((startTime - df.index[0]).total_seconds() // secondsBetweenEntries)

        if endTime is None:
            end = len(df)
        else:
            end = int((endTime - df.index[0]).total_seconds() // secondsBetweenEntries) + 1

        return df[column].values[start:end]

    def _readTickerDataHelper(self, ticker, period):
        listOfDicts = []
        entries = []

        path = self.filePathPrefix + ticker + "-" + period + "-data.csv"
        count = 0
        numSamples = 0
        previousTiming = None

        if period == "1m" or period == "intraminute":
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

                    if timing > self.dateOfEnd:
                        break

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
            elif period == "intraminute":
                self._1MinIntraMinuteDataAsDataFrames[ticker] = df
            else:
                # 1 day
                self._1DayDataAsDataFrames[ticker] = df

            self._addAverage(ticker, period)
            print("Done reading " + ticker + " historical data.")

        except IOError as e:
            print("Could not read " + path + "!")

