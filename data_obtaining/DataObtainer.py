# Name: Data Obtainer
# Author: Robert Ciborowski
# Date: 13/04/2020
# Description: Keeps track of stock prices to the minute, simulated or real
#              clock. This is an abstract class.

from datetime import datetime
from typing import Dict, List
import pandas as pd

from abc import ABCMeta
from abc import abstractmethod

class DataObtainer(metaclass=ABCMeta):
    """
    Lets the obtainer know that the following stocks
    will be requested by use of obtainPrices and obtainPrice.
    """
    @abstractmethod
    def trackTickers(self, tickers: List[str]):
        pass

    @abstractmethod
    def stopTrackingTickers(self, tickers: List[str]):
        pass

    """
    Returns all values, including Timestamp, Close, Volume, in a DataFrame.
    """
    @abstractmethod
    def obtainMinuteValues(self, ticker: str, startTime=None, endTime=None, column=None):
        pass

    """
    Returns all values, including Timestamp, Close, Volume, in a DataFrame.
    """
    @abstractmethod
    def obtainHourValues(self, ticker: str, startTime=None, endTime=None, column=None):
        pass

    """
    Returns all values, including Timestamp, Close, Volume, in a DataFrame.
    """
    @abstractmethod
    def obtainDayValues(self, ticker: str, startTime=None, endTime=None, column=None):
        pass

    """
    Returns the values of just a single column as a numpy array. This should run fast.
    """
    @abstractmethod
    def obtainSingleColumnMinuteValues(self, ticker: str, column, startTime=None, endTime=None):
        pass

    """
    Returns the values of just a single column as a numpy array. This should run fast.
    """
    @abstractmethod
    def obtainSingleColumnHourValues(self, ticker: str, column, startTime=None, endTime=None):
        pass

    """
    Returns the values of just a single column as a numpy array. This should run fast.
    """
    @abstractmethod
    def obtainSingleColumnDayValues(self, ticker: str, column, startTime=None, endTime=None):
        pass

    """
    Adds a custom column to the data. Depending on the subclass, column can be
    a pandas series, lambda function, etc.
    """
    @abstractmethod
    def addCustomMinuteColumn(self, ticker, columnName, column, updateExisting=True):
        pass

    """
    Adds a custom column to the data. Depending on the subclass, column can be
    a pandas series, lambda function, etc.
    """
    @abstractmethod
    def addCustomHourColumn(self, ticker, columnName, column, updateExisting=True):
        pass

    """
    Adds a custom column to the data. Depending on the subclass, column can be
    a pandas series, lambda function, etc.
    """
    @abstractmethod
    def addCustomDayColumn(self, ticker, columnName, column, updateExisting=True):
        pass

    @abstractmethod
    def updateMinuteEntry(self, ticker, columnName, timestamp, value):
        pass

    @abstractmethod
    def updateHourEntry(self, ticker, columnName, timestamp, value):
        pass

    @abstractmethod
    def updateDayEntry(self, ticker, columnName, timestamp, value):
        pass

    """
    Adds an empty column.
    """
    @abstractmethod
    def addEmptyMinuteColumn(self, ticker, columnName, fillValue):
        pass

    """
    Adds an empty column.
    """
    @abstractmethod
    def addEmptyHourColumn(self, ticker, columnName, fillValue):
        pass

    """
    Adds an empty column.
    """
    @abstractmethod
    def addEmptyDayColumn(self, ticker, columnName, fillValue):
        pass

    """
    Removes a custom column.
    """
    @abstractmethod
    def removeCustomMinuteColumn(self, ticker, columnName):
        pass

    """
    Removes a custom column.
    """
    @abstractmethod
    def removeCustomHourColumn(self, ticker, columnName):
        pass

    """
    Removes a custom column.
    """
    @abstractmethod
    def removeCustomDayColumn(self, ticker, columnName):
        pass

    """
    Returns the current minute's price, high and low so far.
    """
    @abstractmethod
    def getIntraMinuteKline(self, tickerPair):
        pass
