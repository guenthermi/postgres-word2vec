#!/usr/bin/python3

import json

class Configuration:
    def __init__(self, filename):
        f = open(filename, 'r')
        self.data = json.loads(f.read())
    def get_value(self, key):
        return self.data[key]
    def has_key(self, key):
        return key in self.data
