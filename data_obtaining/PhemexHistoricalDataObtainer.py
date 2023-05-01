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

class PhemexHistoricalDataObtainer(DataObtainer):
    dateOfStart: datetime
    dateOfEnd: datetime
    filePathPrefix: str
    timezone: str

    # Pandas makes life easy but is very slow, so we use both Pandas and a
    # list of dicts. :clown:
    _1MinDataAsDataFrames: Dict[str, pd.DataFrame]
    _1HourDataAsDataFrames: Dict[str, pd.DataFrame]
    _1DayDataAsDataFrames: Dict[str, pd.DataFrame]
    _obtained: bool

    # For generating a random intra minute price
    _lastIntraMinuteTime: datetime
    _lastIntraMinutePrice: float
    _lastIntraMinuteHigh: float
    _lastIntraMinuteLow: float

    _1MinIntraMinuteDataAsDataFrames: Dict[str, pd.DataFrame]

    def __init__(self, dateOfStart: datetime, dateOfEnd: datetime, filePathPrefix=""):
        self._1MinDataAsDataFrames = {}
        self._1HourDataAsDataFrames = {}
        self._1DayDataAsDataFrames = {}
        self._1MinIntraMinuteDataAsDataFrames = {}
        self._obtained = False
        self.filePathPrefix = filePathPrefix
        self.dateOfStart = dateOfStart
        self.dateOfEnd = dateOfEnd
        self._lastIntraMinuteTime = None
        self._lastIntraMinutePrice = 0.0
        self._lastIntraMinuteHigh = 0.0
        self._lastIntraMinuteLow = 0.0

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

    def getIntraMinuteKline(self, tickerPair):
        minute = Clock.getInstance().getMinuteTimestamp()

        if self._lastIntraMinuteTime == minute:
            return self._lastIntraMinutePrice, self._lastIntraMinuteHigh, self._lastIntraMinuteLow

        self._lastIntraMinuteTime = minute
        valuesMinutes = self._1MinIntraMinuteDataAsDataFrames[tickerPair].loc[minute]
        self._lastIntraMinutePrice = valuesMinutes["Close"]
        self._lastIntraMinuteHigh = valuesMinutes["High"]
        self._lastIntraMinuteLow = valuesMinutes["Low"]
        return self._lastIntraMinutePrice, self._lastIntraMinuteHigh, self._lastIntraMinuteLow

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
                columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"])
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
