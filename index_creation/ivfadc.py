#!/bin/python3

from scipy.cluster.vq import kmeans
from scipy.spatial.distance import sqeuclidean
from scipy.spatial.distance import cdist
import sys
import numpy as np
import faiss
import time
import psycopg2

from config import *
from logger import *
import index_utils as utils

def get_table_information(index_config):
    return ((index_config.get_value('coarse_table_name'),"(id serial PRIMARY KEY, vector float4[], count int)"),
        (index_config.get_value('fine_table_name'),"(id serial PRIMARY KEY, coarse_id integer REFERENCES {!s} (id), word varchar(100), vector int[])".format(index_config.get_value('coarse_table_name'))),
        (index_config.get_value('cb_table_name'), "(id serial PRIMARY KEY, pos int, code int, vector float4[], count int)"))

def create_coarse_quantizer(vectors, centr_num, iters=10):
    centr_map, distortion = kmeans(vectors, centr_num, iters)
    return np.array(centr_map)

def create_fine_quantizer(cq, vectors, m, centr_num, iterts=10):
    if len(vectors[0]) % m != 0:
        logger.log(Logger.ERROR, 'd mod m != 0')
        return
    result = centroids = []
    len_centr = int(len(vectors[0]) / m)

    # create faiss index for coarse quantizer
    index = faiss.IndexFlatL2(len(vectors[0]))
    index.add(cq)

    # partition vectors (each vector)
    partitions = []
    for vec in vectors:
        _, I = index.search(np.array([vec]),1)
        coarse_quantization = cq[I[0][0]]
        residual = vec - coarse_quantization # ! vectors must be numpy arrays
        partitions.append([residual[i:i + len_centr] for i in range(0, len(residual), len_centr)])
    for i in range(m):
        subvecs = [partitions[j][i] for j in range(len(partitions))]
        # apply k-means -> get maps id \to centroid for each partition (use scipy k-means)
        logger.log(Logger.INFO, str(subvecs[0])) # TODO replace info
        centr_map, distortion = kmeans(subvecs, centr_num, iterts) # distortion is unused at the moment
        centroids.append(np.array(centr_map).astype('float32')) #  centr_map could be transformed into a real map (maybe not reasonable)
    return np.array(result) # list of lists of centroids

def create_index_with_faiss(vectors, cq, codebook):
    logger.log(Logger.INFO, 'len of vectors ' + str(len(vectors)))
    result = []
    indices = []
    coarse_counts = dict()
    fine_counts = dict()
    m = len(codebook)
    len_centr = int(len(vectors[0]) / m)

    # create faiss index for coarse quantizer
    coarse = faiss.IndexFlatL2(len(vectors[0]))
    coarse.add(cq)

    # create indices for codebook
    for i in range(m):
        index = faiss.IndexFlatL2(len_centr)
        index.add(codebook[i])
        indices.append(index)
    count = 0
    batches = [[] for i in range(m)]
    coarse_ids = []
    for c in range(len(vectors)):
        count += 1
        vec = vectors[c]
        _, I = coarse.search(np.array([vec]), 1)
        coarse_quantization = cq[I[0][0]]
        coarse_ids.append(I[0][0])

        # update coarse counts
        if I[0][0] in coarse_counts:
            coarse_counts[I[0][0]] += 1
        else:
            coarse_counts[I[0][0]] = 1

        residual = vec - coarse_quantization
        partition = np.array([np.array(residual[i:i + len_centr]).astype('float32') for i in range(0, len(residual), len_centr)])

        time1 = time.time()
        for i in range(m):
            if (time.time - time1) > 60:
                logger.log(Logger.INFO, 'vec ' + str(vec) + ' i ' +  str(i) +  ' m ' + str(m) + ' count ' + str(count))
                time1 += 100000
            batches[i].append(partition[i])
        if (count % 40 == 0) or (c == (len(vectors)-1)): #  seems to be a good value
            size = 40 if (count % 40 == 0) else (c+1) % 40
            codes=[(coarse_ids[i],[]) for i in range(size)]
            for i in range(m):
                _, I = indices[i].search(np.array(batches[i]), 1)
                for j in range(len(codes)):
                    codes[j][1].append(I[j][0])
                    if (i, I[j][0]) in fine_counts:
                        fine_counts[(i, I[j][0])] += 1
                    else:
                        fine_counts[(i, I[j][0])] = 1
            result += codes
            batches = [[] for i in range(m)]
            coarse_ids = []
        if count % 1000 == 0:
            logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
    logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
    return result, coarse_counts, fine_counts

def add_to_database(words, cq, codebook, pq_quantization, coarse_counts, fine_counts, con, cur, index_config, batch_size):
    logger.log(Logger.INFO, 'Length of words: ' + str(len(words)) + ' Length of pq_quantization: ' + str(len(pq_quantization)))
    # add codebook
    for pos in range(len(codebook)):
        values = []
        for i in range(len(codebook[pos])):
            output_vec = utils.serialize_vector(codebook[pos][i])
            count = fine_counts[(pos, i)] if (pos, i) in fine_counts else 0
            values.append({"pos": pos, "code": i, "vector": output_vec, "count": count})
        cur.executemany("INSERT INTO "+ index_config.get_value('cb_table_name') + " (pos,code,vector,count) VALUES (%(pos)s, %(code)s, %(vector)s, %(count)s)", tuple(values))
        con.commit()

    # add coarse quantization
    values = []
    for i in range(len(cq)):#
        output_vec = utils.serialize_vector(cq[i])
        count = coarse_counts[i] if i in coarse_counts else 0
        values.append({"id": i, "vector": output_vec, "count": count})
    cur.executemany("INSERT INTO " + index_config.get_value('coarse_table_name') + " (id, vector, count) VALUES (%(id)s, %(vector)s, %(count)s)", tuple(values))
    con.commit()

    # add fine qunatization
    values = []
    for i in range(len(pq_quantization)):
        output_vec = utils.serialize_vector(pq_quantization[i][1])
        values.append({"coarse_id": str(pq_quantization[i][0]), "word": words[i][:100], "vector": output_vec})
        if (i % (batch_size-1) == 0) or (i == (len(pq_quantization)-1)):
            cur.executemany("INSERT INTO "+ index_config.get_value('fine_table_name') + " (coarse_id, word,vector) VALUES (%(coarse_id)s, %(word)s, %(vector)s)", tuple(values))
            con.commit()
            logger.log(Logger.INFO, 'Inserted ' +  str(i+1) + ' vectors')
            values = []
    return

def main(argc, argv):
    db_config = Configuration('db_config.json')
    logger = Logger(db_config.get_value('log'))
    if argc < 2:
        logger.log(Logger.ERROR, 'Configuration file for index creation required')
        return
    index_config = Configuration(argv[1])

    batch_size = db_config.get_value("batch_size")

    train_size_coarse = index_config.get_value('train_size_coarse')
    train_size_fine = index_config.get_value('train_size_fine')
    centr_num_coarse = index_config.get_value('k_coarse')
    m = index_config.get_value('m')
    k = index_config.get_value('k')

    # get vectors
    words, vectors, vectors_size = utils.get_vectors(index_config.get_value('vec_file_path'), logger)
    logger.log(logger.INFO, 'vectors_size :' + vectors_size)

    # create coarse quantizer
    cq = create_coarse_quantizer(vectors[:train_size_coarse], centr_num_coarse)

    # calculate codebook based on residuals
    codebook = create_fine_quantizer(cq, vectors[:train_size_fine], m, k)

    # create index with qunatizers
    start = time.time()
    index, coarse_counts, fine_counts = create_index_with_faiss(vectors[:vectors_size], cq, codebook)
    end = time.time()
    logger.log(logger.INFO, 'Finish index creation after ' + str(end - start) + ' seconds')

    # add to file
    if (index_config.get_value('export_to_file')):
        index_data = dict({
            'words': words,
            'codebook': codebook,
            'index': index,
            'counts': counts
        })
        im.save_index(index_data, index_config.get_value('export_name'))

    if (index_config.get_value('add_to_database')):
        # create db connection
        try:
            con = psycopg2.connect("dbname='" + db_config.get_value('db_name') + "' user='" + db_config.get_value('username') + "' host='" + db_config.get_value('host') + "' password='" + db_config.get_value('password') + "'")
        except:
            logger.log(logger.ERROR, 'Can not connect to database')
            return
        cur = con.cursor()

        utils.init_tables(con, cur, get_table_information(), logger)

        add_to_database(words, cq, codebook, index, coarse_counts, fine_counts, con, cur, batch_size)

        utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_word_index_name'), 'word', con, cur, logger)
        utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_coarse_index_name'), 'coarse_id', con, cur, logger)

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
