#!/bin/python3

import pickle

def _load_file(filename):
    f = open(filename, 'rb')
    result = pickle.load(f)
    f.close()
    return result

def load_index(filename):
    return _load_file(filename)

def save_index(data, filename):
    output = open(filename, 'wb')
    pickle.dump(data, output)
    output.close()
    return

def load_pipeline_ivfadc_index(filename_data, filename_counts, filename_cq, filename_codebook):
    counts = _load_file(filename_counts)
    cq = _load_file(filename_cq)
    cb = _load_file(filename_codebook)

    # collect index data
    words = []
    index = []
    data_file = open(filename_data, 'rb')
    while (True):
        try:
            batch = pickle.load(data_file)
            words += batch['words']
            index += batch['index']
        except EOFError:
            break

    return {
        'index': index,
        'codebook': cb,
        'words': words,
        'fine_counts': counts['fine_counts'],
        'coarse_counts': counts['coarse_counts'],
        'cq': cq
    }

def load_pipeline_pq_index(filename_data, filename_counts, filename_codebook):
    counts = _load_file(filename_counts)
    cb = _load_file(filename_codebook)

    # collect index data
    words = []
    index = []
    data_file = open(filename_data, 'rb')
    while (True):
        try:
            batch = pickle.load(data_file)
            words += batch['words']
            index += batch['index']
        except EOFError:
            break

    return {
        'words': words,
        'codebook': cb,
        'index': index,
        'counts': counts
    }
