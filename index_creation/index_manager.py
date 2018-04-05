#!/bin/python3

import pickle

def load_index(filename):
    f = open(filename, 'rb')
    return pickle.load(f)

def save_index(data, filename):
    output = open(filename, 'wb')
    pickle.dump(data, output)
    return
