#!/bin/python3

from scipy.cluster.vq import kmeans
from scipy.spatial.distance import sqeuclidean
from scipy.spatial.distance import cdist
import sys
import numpy as np
import faiss
import time
import psycopg2

import index_utils as utils

STD_USER = 'postgres'
STD_PASSWORD = 'postgres'
STD_HOST = 'localhost'
STD_DB_NAME = 'imdb'

BATCH_SIZE = 50000

COARSE_TABLE_NAME = 'coarse_quantization'
CODEBOOK_TABLE_NAME = 'residual_codebook'
FINE_TABLE_NAME = 'fine_quantization'

WORD_INDEX_NAME = 'fine_quantization_word_idx'
COARSE_INDEX_NAME = 'fine_quantization_coarse_id_idx'

VEC_FILE_PATH = '../vectors/google_vecs.txt'

def get_table_information():
    return ((COARSE_TABLE_NAME,"(id serial PRIMARY KEY, vector float4[], count int)"),
        (FINE_TABLE_NAME,"(id serial PRIMARY KEY, coarse_id integer REFERENCES {!s} (id), word varchar(100), vector int[])".format(COARSE_TABLE_NAME)),
        (CODEBOOK_TABLE_NAME, "(id serial PRIMARY KEY, pos int, code int, vector float4[], count int)"))

def create_coarse_quantizer(vectors, centr_num, iters=10):
    centr_map, distortion = kmeans(vectors, centr_num, iters)
    return np.array(centr_map)

def create_fine_quantizer(cq, vectors, m, centr_num, iterts=10):
    if len(vectors[0]) % m != 0:
        print('Error d mod m != 0')
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
        print(subvecs[0])
        centr_map, distortion = kmeans(subvecs, centr_num, iterts) # distortion is unused at the moment
        centroids.append(np.array(centr_map).astype('float32')) #  centr_map could be transformed into a real map (maybe not reasonable)
    return np.array(result) # list of lists of centroids

def create_index_with_faiss(vectors, cq, codebook):
    print('len vectors', len(vectors))
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

        for i in range(m):
            batches[i].append(partition[i])
        if (count % 18 == 0) or (c == (len(vectors)-1)): # 18 seems to be a good value
            size = 18 if (count % 18 == 0) else (c+1) % 18
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
            print('appended', len(result), 'vectors')
    print('appended', len(result), 'vectors')
    return result, coarse_counts, fine_counts

def add_to_database(words, cq, codebook, pq_quantization, coarse_counts, fine_counts, con, cur):
    print('len words', len(words), 'len pq_quantization', len(pq_quantization))
    # add codebook
    for pos in range(len(codebook)):
        values = []
        for i in range(len(codebook[pos])):
            output_vec = utils.serialize_vector(codebook[pos][i])
            count = fine_counts[(pos, i)] if (pos, i) in fine_counts else 0
            values.append({"pos": pos, "code": i, "vector": output_vec, "count": count})
        cur.executemany("INSERT INTO "+ CODEBOOK_TABLE_NAME + " (pos,code,vector,count) VALUES (%(pos)s, %(code)s, %(vector)s, %(count)s)", tuple(values))
        con.commit()

    # add coarse quantization
    values = []
    for i in range(len(cq)):#
        output_vec = utils.serialize_vector(cq[i])
        count = coarse_counts[i] if i in coarse_counts else 0
        values.append({"id": i, "vector": output_vec, "count": count})
    cur.executemany("INSERT INTO " + COARSE_TABLE_NAME + " (id, vector, count) VALUES (%(id)s, %(vector)s, %(count)s)", tuple(values))
    con.commit()

    # add fine qunatization
    values = []
    for i in range(len(pq_quantization)):
        output_vec = utils.serialize_vector(pq_quantization[i][1])
        values.append({"coarse_id": str(pq_quantization[i][0]), "word": words[i][:100], "vector": output_vec})
        if (i % (BATCH_SIZE-1) == 0) or (i == (len(pq_quantization)-1)):
            cur.executemany("INSERT INTO "+ FINE_TABLE_NAME + " (coarse_id, word,vector) VALUES (%(coarse_id)s, %(word)s, %(vector)s)", tuple(values))
            con.commit()
            print('Inserted', i+1, 'vectors')
            values = []
    return

def main(argc, argv):
    global VEC_FILE_PATH, COARSE_TABLE_NAME, CODEBOOK_TABLE_NAME, FINE_TABLE_NAME, WORD_INDEX_NAME, COARSE_INDEX_NAME
    train_size_coarse = 100000
    train_size_fine = 100000
    centr_num_coarse = 1000
    m = 12
    k = 256

    if argc > 1:
        VEC_FILE_PATH = argv[1]
    if argc > 4:
        COARSE_TABLE_NAME = argv[2]
        CODEBOOK_TABLE_NAME = argv[3]
        FINE_TABLE_NAME = argv[4]
    if argc > 6:
        WORD_INDEX_NAME = argv[5]
        COARSE_INDEX_NAME = argv[6]
    if argc > 9:
        m = int(argv[7])
        k = int(argv[8])
        centr_num_coarse = int(argv[9])

    # get vectors
    words, vectors, vectors_size = utils.get_vectors(VEC_FILE_PATH)
    print(vectors_size)

    # create coarse quantizer
    cq = create_coarse_quantizer(vectors[:train_size_coarse], centr_num_coarse)

    # calculate codebook based on residuals
    codebook = create_fine_quantizer(cq, vectors[:train_size_fine], m, k)

    # create index with qunatizers
    start = time.time()
    index, coarse_counts, fine_counts = create_index_with_faiss(vectors[:vectors_size], cq, codebook)
    end = time.time()
    print('finish index creation after', end - start, 'seconds')

    # create db connection
    try:
        con = psycopg2.connect("dbname='" + STD_DB_NAME + "' user='" + STD_USER + "' host='" + STD_HOST + "' password='" + STD_PASSWORD + "'")
    except:
        print('Can not connect to database')
        return
    cur = con.cursor()

    utils.init_tables(con, cur, get_table_information())

    add_to_database(words, cq, codebook, index, coarse_counts, fine_counts, con, cur)

    utils.create_index(FINE_TABLE_NAME, WORD_INDEX_NAME, 'word', con, cur)
    utils.create_index(FINE_TABLE_NAME, COARSE_INDEX_NAME, 'coarse_id', con, cur)

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
