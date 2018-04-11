#!/usr/bin/python3

import numpy as np
import faiss
import time

from logger import *

class IVFADCIndexCreator:
    def __init__(self, cq, codebook, m, len_centr, logger):
        self.logger = logger
        self.cq = cq

        self.indices = []
        self.coarse_counts = dict()
        self.fine_counts = dict()#
        self.m = m
        self.len_centr = len_centr

        # create faiss index for coarse quantizer
        self.coarse = faiss.IndexFlatL2(m * len_centr)
        self.coarse.add(cq)

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
            _, I = self.coarse.search(np.array([vec]), 1)
            coarse_quantization = self.cq[I[0][0]]
            coarse_ids.append(I[0][0])

            # update coarse counts
            if I[0][0] in self.coarse_counts:
                self.coarse_counts[I[0][0]] += 1
            else:
                self.coarse_counts[I[0][0]] = 1

            residual = vec - coarse_quantization
            partition = np.array([np.array(residual[i:i + self.len_centr]).astype('float32') for i in range(0, len(residual), self.len_centr)])

            time1 = time.time()
            for i in range(self.m):
                if (time.time() - time1) > 60:
                    self.logger.log(Logger.INFO, 'vec ' + str(vec) + ' i ' +  str(i) +  ' m ' + str(self.m) + ' count ' + str(count))
                    time1 += 100000
                batches[i].append(partition[i])
            if (count % 40 == 0) or (c == (len(batch)-1)): #  seems to be a good value
                size = 40 if (count % 40 == 0) else (c+1) % 40
                codes=[(coarse_ids[i],[]) for i in range(size)]
                for i in range(self.m):
                    _, I = self.indices[i].search(np.array(batches[i]), 1)
                    for j in range(len(codes)):
                        codes[j][1].append(I[j][0])
                        if (i, I[j][0]) in self.fine_counts:
                            self.fine_counts[(i, I[j][0])] += 1
                        else:
                            self.fine_counts[(i, I[j][0])] = 1
                result += codes
                batches = [[] for i in range(self.m)]
                coarse_ids = []
            if count % 1000 == 0:
                self.logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
        self.logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
        return result, self.coarse_counts, self.fine_counts

    def has_next(self):
        return self.cursor < len(self.data)
    def get_cursor(self):
        return self.cursor
