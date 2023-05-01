from clock.Clock import Clock
from datetime import datetime, timedelta


class SimulatedClock(Clock):
    _currentTimestamp: datetime

    def __init__(self, startTimestamp):
        super().__init__()
        self._currentTimestamp = startTimestamp

    def getTimestamp(self) -> datetime:
        return self._currentTimestamp

    def advance(self, timedelta):
        self._currentTimestamp += timedelta

    def advanceByMinute(self):
        self._currentTimestamp += timedelta(minutes=1)

    def changeTime(self, timestamp: datetime):
        self._currentTimestamp = timestamp
