#!/bin/python3

from scipy.cluster.vq import kmeans
import pickle
import numpy as np

from  logger import *


def create_quantizer(vectors, m, centr_num, logger, iterts=10):
    if len(vectors[0]) % m != 0:
        logger.log(Logger.ERROR, 'd mod m != 0')
        return
    result = centroids = []
    len_centr = int(len(vectors[0]) / m)
    # partition vectors (each vector)
    partitions = []
    for vec in vectors:
        partitions.append([vec[i:i + len_centr] for i in range(0, len(vec), len_centr)])
    for i in range(m):
        subvecs = [partitions[j][i] for j in range(len(partitions))]
        # apply k-means -> get maps id \to centroid for each partition (use scipy k-means)
        logger.log(Logger.INFO, str(subvecs[0])) # TODO replace info
        centr_map, distortion = kmeans(subvecs, centr_num, iterts) # distortion is unused at the moment
        centroids.append(np.array(centr_map).astype('float32')) #  centr_map could be transformed into a real map (maybe not reasonable)
    return np.array(result) # list of lists of centroids

def create_coarse_quantizer(vectors, centr_num, iters=10):
    centr_map, distortion = kmeans(vectors, centr_num, iters)
    return np.array(centr_map)

def store_quantizer(centroids, filename):
    output = open(filename, 'wb')
    pickle.dump(centroids, output)
    return

def load_quantizer(filename):
    f = open(filename, 'rb')
    return pickle.load(f)
