#!/bin/python3

import pickle

def load_index(filename):
    f = open(filename)
    self.data = pickle.load(f)
    return self.data

def save_index(data, filename):
    output = open(filename, 'w')
    pickle.dump(data, output)
    return
