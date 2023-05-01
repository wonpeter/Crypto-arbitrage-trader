import sys
import time


class Logger:
    def __init__(self, filename: str, secondaryFiles={}, saveInterval=60):
        self.terminal = sys.stdout
        self.filename = filename
        self.saveInterval = saveInterval
        self.log = open(filename, "w")
        self.log.write("")

        self.secondaryLogs = {}
        self.lastSecondarySaveTimes = {}
        self.secondaryFiles = secondaryFiles

        for tag, path in secondaryFiles.items():
            self.secondaryLogs[tag] = open(path, "w")
            self.secondaryLogs[tag].write("")

        self._saveAll()

    def write(self, message: str):
        self.terminal.write(message)

        written = self.log.write(message)

        if written != len(message):
            # Try to close the file and try again.
            if self.log.closed:
                self.log = open(self.filename, "a")
                self.lastSaveTime = time.time()

            written += self.log.write(message[written:])

            if written != len(message):
                # Shoot.
                pass
                # raise Exception("Logger: could not write " + message + " to "\
                #     "write(), only wrote " + str(written) + " elements.")

        if self.lastSaveTime + self.saveInterval < time.time():
            self._saveAll()

    def writeSecondary(self, tag: str, message: str):
        # Just in case, because sometimes files close by themselves??
        if self.secondaryLogs[tag].closed:
            self.secondaryLogs[tag] = open(self.secondaryFiles[tag], "a")

        message = message + "\n"
        written = self.secondaryLogs[tag].write(message)

        if written != len(message):
            # Try to close the file and try again.
            if self.secondaryLogs[tag].closed:
                self.secondaryLogs[tag] = open(self.secondaryFiles[tag], "a")

            written += self.secondaryLogs[tag].write(message[written:])

            if written != len(message):
                # Shoot.
                # raise Exception("Logger: could not write " + message + " to "
                #     + tag + ", only wrote " + str(written) + " elements.")
                print("Logger: could not write " + message + " to "
                      + tag + ", only wrote " + str(written) + " elements.")

        if self.lastSaveTime + self.saveInterval < time.time():
            self._saveAll()

    def flush(self):
        pass

    def isatty(self):
        return False

    def _saveAll(self):
        if not self.log.closed:
            self.log.close()

        self.log = open(self.filename, "a")
        self.lastSaveTime = time.time()

        for tag in self.secondaryLogs:
            self._saveSecondary(tag)

    def _saveSecondary(self, tag: str):
        if not self.secondaryLogs[tag].closed:
            self.secondaryLogs[tag].close()

        self.secondaryLogs[tag] = open(self.secondaryFiles[tag], "a")
        self.lastSecondarySaveTimes[tag] = time.time()
