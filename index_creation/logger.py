#!/usr/bin/python3

import sys
import time
import datetime

TIME_STAMPS = True

class Logger:

    LEVELS = INFO, WARNING, ERROR = range(3)

    LEVEL_NAMES = {
        INFO: 'INFO',
        WARNING: 'WARNING',
        ERROR: 'ERROR'
    }

    # TODO option to filter messages according to there log level

    def __init__(self, filename='log.out'):
        self.filename = filename
        if filename == "":
            self.stdout = True
            self.fileout = False
            self.dest_file = None
        else:
            self.dest_file = open(filename, 'w')
            self.stdout = False
            self.fileout = True
            self.stderr = f
    def set_stdout(self, valid):
        self.stdout = valid
    def set_fileout(self, valid):
        self.fileouot = valid
    def log(self, level, message):
        output = Logger.LEVEL_NAMES[level] + (' [' + str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')) + '] ' if TIME_STAMPS else '') + ': ' + message + '\n'
        if self.stdout:
            print(output)
        if self.fileout:
            self.dest_file.write(output)
