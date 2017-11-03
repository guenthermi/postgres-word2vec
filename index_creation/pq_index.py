#!/bin/python3
from scipy.cluster.vq import kmeans
from scipy.spatial.distance import sqeuclidean
from scipy.spatial.distance import cdist
import sys
import numpy as np
import faiss
import time
import psycopg2

STD_USER = 'postgres'
STD_PASSWORD = 'postgres'
STD_HOST = 'localhost'
STD_DB_NAME = 'imdb'

BATCH_SIZE = 50000

PQ_TABLE_NAME = 'pq_quantization'
CODEBOOK_TABLE_NAME = 'pq_codebook'
VEC_FILE_PATH = '../vectors/google_vecs.txt'

def get_vectors(filename, max_count=10**9):
    f = open(filename)
    line_splits = f.readline().split()
    size = int(line_splits[0])
    d = int(line_splits[1])
    words, vectors, count = [],np.zeros((size, d)).astype('float32'), 0
    print(count)
    print(line_splits)
    while (line_splits) and (count < max_count):
        line_splits = f.readline().split()
        if not line_splits:
            break
        word = line_splits[0]
        vector = [float(elem) for elem in line_splits[1:]]
        v_len = np.linalg.norm(vector)
        vector = [x / v_len for x in vector]
        if len(vector) == 300:
            vectors[count] = vector
            words.append(word)
            # vectors.append(vector)
            # words.append(word)
            count += 1
        if count % 10000 == 0:
            print('INFO read', count, 'vectors')
    return words, vectors, count

def create_quantizer(vectors, m, centr_num, iterts=10):
    if len(vectors[0]) % m != 0:
        print('Error d mod m != 0')
        return
    result = centroids = []
    len_centr = int(len(vectors[0]) / m)
    # 2) partition vectors (each vector)
    partitions = []
    for vec in vectors:
        partitions.append([vec[i:i + len_centr] for i in range(0, len(vec), len_centr)])
    for i in range(m):
        subvecs = [partitions[j][i] for j in range(len(partitions))]
        # 3) apply k-means -> get maps id \to centroid for each partition (use scipy k-means)
        print(subvecs[0])
        centr_map, distortion = kmeans(subvecs, centr_num, iterts) # distortion is unused at the moment
        centroids.append(np.array(centr_map).astype('float32')) #  centr_map could be transformed into a real map (maybe not reasonable)
    return np.array(result) # list of lists of centroids

def create_index_with_faiss(vectors, codebook):
    print('len vectors', len(vectors))
    result = []
    indices = []
    m = len(codebook)
    len_centr = int(len(vectors[0]) / m)
    # create indices for codebook
    for i in range(m):
        index = faiss.IndexFlatL2(len_centr)
        print(codebook[i])
        index.add(codebook[i])
        indices.append(index)
    count = 0
    batches = [[] for i in range(m)]
    for c in range(len(vectors)):
        count += 1
        vec = vectors[c]
        partition = np.array([np.array(vec[i:i + len_centr]).astype('float32') for i in range(0, len(vec), len_centr)])
        for i in range(m):
            batches[i].append(partition[i])
        if (count % 18 == 0) or (c == (len(vectors)-1)): # 18 seems to be a good value
            size = 18 if (count % 18 == 0) else (c+1) % 18
            codes=[[] for i in range(size)]
            for i in range(m):
                _, I = indices[i].search(np.array(batches[i]), 1)
                for j in range(len(codes)):
                    codes[j].append(I[j][0])
            result += codes
            batches = [[] for i in range(m)]
        if count % 1000 == 0:
            print('appended', len(result), 'vectors')
        # result.append(code)
    print('appended', len(result), 'vectors')
    return result

def create_index(vectors, codebook):
    result = []
    m = len(codebook)
    len_centr = int(len(vectors[0]) / m)
    count = 0
    for vec in vectors:
        code = []
        # partition vector
        partition = np.array([np.array(vec[i:i + len_centr]) for i in range(0, len(vec), len_centr)]).astype('float32')
        # determine nearest centroide from the codebook for each partition
        #  -> generate code
        for i in range(m):
            min_dist = None
            code_id = None
            for j in range(len(codebook[i])):
                c = codebook[i][j]
                # calculate dist
                dist = np.linalg.norm(partition[i] - codebook[i][j])
                if (not min_dist) or (dist < min_dist):
                    min_dist = dist
                    code_id = j
            code.append(code_id)
        # add code to result
        count += 1
        result.append(code)
        if count % 100 == 0:
            print('append', count, 'vectors')
    return result

def init_tables(con, cur):
    query_check_existence = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=%s;"
    for (name, schema) in ((PQ_TABLE_NAME,"(id serial PRIMARY KEY, word varchar(100), vector int[])") ,(CODEBOOK_TABLE_NAME, "(id serial PRIMARY KEY, pos int, code int, vector float4[])")):
        cur.execute(cur.mogrify(query_check_existence, (name,)))
        rows = cur.fetchall()
        table_exists = (len(rows) > 0)
        if not table_exists:
            query_create_table = "CREATE TABLE " + name + " " + schema + ";"
            result = cur.execute(query_create_table)
            # commit changes
            con.commit()
            print('Created new table', name)
        else:
            # delete content of table
            query_clear = "DELETE FROM " + name + ";"
            result = cur.execute(query_clear)
            con.commit()
            print('Table', name, 'already exists. All entries are removed.')
    return

def add_to_database(words, codebook, pq_quantization, con, cur):
    print('len words', len(words), 'len pq_quantization', len(pq_quantization))
    # add codebook
    for pos in range(len(codebook)):
        values = []
        for i in range(len(codebook[pos])):
            vec = codebook[pos][i]
            output_vec = '{'
            for elem in vec:
                output_vec += str(elem) + ','
            output_vec = output_vec[:-1] + '}'
            values.append({"pos": pos, "code": i, "vector": output_vec})
        cur.executemany("INSERT INTO "+ CODEBOOK_TABLE_NAME + " (pos,code,vector) VALUES (%(pos)s, %(code)s, %(vector)s)", tuple(values))
        con.commit()

    # add pq qunatization
    values = []
    for i in range(len(pq_quantization)):
        output_vec = '{'
        for elem in pq_quantization[i]:
            output_vec += str(elem) + ','
        output_vec = output_vec[:-1] + '}'
        values.append({"word": words[i], "vector": output_vec})
        if (i % (BATCH_SIZE-1) == 0) or (i == (len(pq_quantization)-1)):
            cur.executemany("INSERT INTO "+ PQ_TABLE_NAME + " (word,vector) VALUES (%(word)s, %(vector)s)", tuple(values))
            con.commit()
            print('Inserted', i+1, 'vectors')
            values = []
    return

def main(argc, argv):
    train_size = 10000
    # 1) get vectors
    words, vectors, vectors_size = get_vectors(VEC_FILE_PATH)
    print(vectors_size)
    # 2) select samples for training
    # 3) partition sample vectors (each vector)
    # 4) apply k-means -> get maps id \to centroid for each partition (use scipy k-means)
    codebook = create_quantizer(vectors[:train_size], 12, 256)
    # 5) create index with qunatizer
    start = time.time()
    index = create_index_with_faiss(vectors[:vectors_size], codebook)
    end = time.time()
    print('finish index creation after', end - start, 'seconds')
    # create db connection
    try:
        con = psycopg2.connect("dbname='" + STD_DB_NAME + "' user='" + STD_USER + "' host='" + STD_HOST + "' password='" + STD_PASSWORD + "'")
    except:
        print('Can not connect to database')
        return
    cur = con.cursor()
    init_tables(con, cur)
    add_to_database(words, codebook, index, con, cur)

    # 4) write vectors to database
    # *) implement funciton to calculate distances
if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
