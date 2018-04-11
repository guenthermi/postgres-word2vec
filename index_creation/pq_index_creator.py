#!/usr/bin/python3

import numpy as np
import faiss
import time

from logger import *

class PQIndexCreator:
    def __init__(self, codebook, m, len_centr, logger):
        self.logger = logger

        self.indices = []
        self.counts = dict()
        self.m = m
        self.len_centr = len_centr

        # create indices for codebook
        for i in range(m):
            index = faiss.IndexFlatL2(len_centr)
            index.add(codebook[i])
            self.indices.append(index)

    def index_batch(self, batch):
        count = 0
        result = []
        batches = [[] for i in range(self.m)]
        coarse_ids = []
        for c in range(len(batch)):
            count += 1
            vec = batch[c]
            partition = np.array([np.array(vec[i:i + self.len_centr]).astype('float32') for i in range(0, len(vec), self.len_centr)])

            time1 = time.time()
            for i in range(self.m):
                if (time.time() - time1) > 60:
                    self.logger.log(Logger.INFO, 'vec ' + str(vec) + ' i ' +  str(i) +  ' m ' + str(self.m) + ' count ' + str(count))
                    time1 += 100000
                batches[i].append(partition[i])
            if (count % 40 == 0) or (c == (len(batch)-1)): #  seems to be a good value
                size = 40 if (count % 40 == 0) else (c+1) % 40
                codes=[[] for i in range(size)]
                for i in range(self.m):
                    _, I = self.indices[i].search(np.array(batches[i]), 1)
                    for j in range(len(codes)):
                        codes[j].append(I[j][0])
                        # update counts
                        if (i, I[j][0]) in self.counts:
                            self.counts[(i, I[j][0])] += 1
                        else:
                            self.counts[(i, I[j][0])] = 1
                result += codes
                batches = [[] for i in range(self.m)]
            if count % 1000 == 0:
                self.logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
        self.logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
        return result, self.counts

    def has_next(self):
        return self.cursor < len(self.data)
    def get_cursor(self):
        return self.cursor
