from abc import ABCMeta
from abc import abstractmethod
from datetime import datetime

"""
A singleton representing a clock.
"""
class Clock(metaclass=ABCMeta):
    _instance = None

    def __init__(self):
        if not Clock._instance:
            Clock._instance = self
        else:
            print("Only one instance of Clock is allowed!")

        self._listeners = {}

    @staticmethod
    def getInstance():
        if not Clock._instance:
            return Clock()
        else:
            return Clock._instance

    @abstractmethod
    def getTimestamp(self) -> datetime:
        pass

    def getMinuteTimestamp(self) -> datetime:
        timestamp = self.getTimestamp()
        timestamp = timestamp.replace(second=0)
        timestamp = timestamp.replace(microsecond=0)
        return timestamp

    def getHourTimestamp(self) -> datetime:
        timestamp = self.getMinuteTimestamp()
        timestamp = timestamp.replace(minute=0)
        return timestamp

    def getDayTimestamp(self) -> datetime:
        timestamp = self.getHourTimestamp()
        timestamp = timestamp.replace(hour=0)
        return timestamp
