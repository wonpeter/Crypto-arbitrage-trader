from clock.Clock import Clock
from datetime import datetime

class RealtimeClock(Clock):
    def getTimestamp(self) -> datetime:
        return datetime.utcnow()
