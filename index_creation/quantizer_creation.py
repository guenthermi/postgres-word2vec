#!/bin/python3

from scipy.cluster.vq import kmeans
import os.path
import pickle
import faiss
import numpy as np


from  logger import *


def create_quantizer(vectors, m, centr_num, logger, iters=10):
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
        centr_map, distortion = kmeans(subvecs, centr_num, iters) # distortion is unused at the moment
        centroids.append(np.array(centr_map).astype('float32')) #  centr_map could be transformed into a real map (maybe not reasonable)
    return np.array(result) # list of lists of centroids

def create_coarse_quantizer(vectors, centr_num, iters=10):
    centr_map, distortion = kmeans(vectors, centr_num, iters)
    return np.array(centr_map)

def create_residual_quantizer(cq, vectors, m, centr_num, logger, iters=10):
    if len(vectors[0]) % m != 0:
        logger.log(Logger.ERROR, 'd mod m != 0')
        return

    # create faiss index for coarse quantizer
    index = faiss.IndexFlatL2(len(vectors[0]))
    index.add(cq)

    # calculate residual for every vector
    residuals = []
    for vec in vectors:
        _, I = index.search(np.array([vec]),1)
        coarse_quantization = cq[I[0][0]]
        residuals.append(vec - coarse_quantization)

    # calculate and return residual codebook
    return create_quantizer(residuals, m, centr_num, logger, iters=iters)

def construct_quantizer(create_func, params, logger, input_name=None, output_name=None):
    quantizer = None
    if input_name:
        if os.path.isfile(input_name):
            logger.log(Logger.INFO, 'Use quantizer from ' + input_name)
            quantizer = load_quantizer(input_name)
    print('type', type(quantizer))
    if type(quantizer) == type(None):
        logger.log(Logger.INFO, 'Create new quantizer for ' + output_name)
        # create coarse quantizer
        quantizer = create_func(*params)
        print('get here')
        print(quantizer[0])
        # store coarse quantizer
        if output_name:
            store_quantizer(quantizer, output_name)
    return quantizer

def store_quantizer(centroids, filename):
    output = open(filename, 'wb')
    pickle.dump(centroids, output)
    return

def load_quantizer(filename):
    f = open(filename, 'rb')
    return pickle.load(f)
