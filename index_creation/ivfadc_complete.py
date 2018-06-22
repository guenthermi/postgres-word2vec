#!/bin/python3

from scipy.cluster.vq import kmeans
from scipy.spatial.distance import sqeuclidean
from scipy.spatial.distance import cdist
import sys
import numpy as np
import faiss
import time
import psycopg2
import pickle

from config import *
from logger import *
from vector_feeder import *
from ivfadc_index_creator import *
import index_utils as utils
import index_manager as im
import quantizer_creation as qcreator


USE_PIPELINE_APPROACH = True
USE_BYTEA_TYPE = True

def get_table_information(index_config):
    if USE_BYTEA_TYPE:
        return ((index_config.get_value('coarse_table_name'),"(id serial PRIMARY KEY, vector bytea, count int)"),
            (index_config.get_value('fine_table_name'),"(id serial PRIMARY KEY, coarse_tag integer, word_id integer, vector bytea)"),
            (index_config.get_value('cb_table_name'), "(id serial PRIMARY KEY, pos int, code int, vector bytea, count int)"))
    else:
        return ((index_config.get_value('coarse_table_name'),"(id serial PRIMARY KEY, vector float4[], count int)"),
            (index_config.get_value('fine_table_name'),"(id serial PRIMARY KEY, coarse_tag integer, word_id integer, vector int[])"),
            (index_config.get_value('cb_table_name'), "(id serial PRIMARY KEY, pos int, code int, vector float4[], count int)"))


def create_fine_quantizer(cq, vectors, m, centr_num, logger, iterts=10):
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
    return qcreator.create_quantizer(residuals, m, centr_num, logger, iterts)

def create_index_with_faiss(vectors, cq, codebook, logger):
    f = open('log_coarse.log', 'wt')
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
    orders = []
    # coarse_ids = []
    batch_coarse_ids = []
    for c in range(len(vectors)):
        # count += 1
        vec = vectors[c]
        # print('len(cq)', len(cq))
        _, I = coarse.search(np.array([vec]), len(cq))

        # coarse_ids.append(I[0]) #??

        # update coarse counts
        if I[0][0] in coarse_counts:
            coarse_counts[I[0][0]] += 1
        else:
            coarse_counts[I[0][0]] = 1
        f.write(str(I[0]) + "##" + str(c) + "##" + str(vec) +  '\n')
        for order, id in enumerate(I[0]):
            coarse_quantization = cq[id]
            batch_coarse_ids.append(id)
            residual = vec - coarse_quantization
            count += 1
            partition = np.array([np.array(residual[i:i + len_centr]).astype('float32') for i in range(0, len(residual), len_centr)])
            time1 = time.time()
            for i in range(m):
                if (time.time() - time1) > 60:
                    logger.log(Logger.INFO, 'vec ' + str(vec) + ' i ' +  str(i) +  ' m ' + str(m) + ' count ' + str(count))
                    time1 += 100000
                batches[i].append(partition[i])
            orders.append(order)
            if (count % 18 == 0) or (count == (len(vectors)*len(cq))): #  seems to be a good value
                # print('count', count, 'c', c,'order', order, 'batch_id_size', len(batch_coarse_ids))
                size = 18 if (count % 18 == 0) else len(batch_coarse_ids)
                # print('size', size, 'orders', orders, 'batch_size', len(batches[0]))
                #print('size', size, coarse_ids, 'c', c, len(vectors))
                # print('batch_coarse_ids[i]',batch_coarse_ids[i])
                codes=[(batch_coarse_ids[i],[], orders[i]) for i in range(size)]
                for i in range(m):
                    _, I = indices[i].search(np.array(batches[i]), 1)
                    for j in range(len(codes)):
                        codes[j][1].append(I[j][0])
                        if order == 0:
                            if (i, I[j][0]) in fine_counts:
                                fine_counts[(i, I[j][0])] += 1
                            else:
                                fine_counts[(i, I[j][0])] = 1
                result += codes
                batches = [[] for i in range(m)]
                orders = []
                batch_coarse_ids = []
                # if order == (len(cq)-1):
                #     coarse_ids = []
            if count % 1000 == 0:
                logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
    logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
    return result, coarse_counts, fine_counts

def add_codebook_to_database(codebook, fine_counts, con, cur, index_config):
    for pos in range(len(codebook)):
        values = []
        for i in range(len(codebook[pos])):
            output_vec = utils.serialize_vector(codebook[pos][i])
            count = fine_counts[(pos, i)] if (pos, i) in fine_counts else 0
            values.append({"pos": pos, "code": i, "vector": output_vec, "count": count})
        if USE_BYTEA_TYPE:
            cur.executemany("INSERT INTO "+ index_config.get_value('cb_table_name') + " (pos,code,vector,count) VALUES (%(pos)s, %(code)s, vec_to_bytea(%(vector)s::float4[]), %(count)s)", tuple(values))
        else:
            cur.executemany("INSERT INTO "+ index_config.get_value('cb_table_name') + " (pos,code,vector,count) VALUES (%(pos)s, %(code)s, %(vector)s, %(count)s)", tuple(values))
        con.commit()
    return

def add_cq_to_database(cq, coarse_counts, con, cur, index_config):
    # add coarse quantization
    values = []
    for i in range(len(cq)):#
        output_vec = utils.serialize_vector(cq[i])
        count = coarse_counts[i] if i in coarse_counts else 0
        values.append({"id": i, "vector": output_vec, "count": count})
    if USE_BYTEA_TYPE:
        cur.executemany("INSERT INTO " + index_config.get_value('coarse_table_name') + " (id, vector, count) VALUES (%(id)s, vec_to_bytea(%(vector)s::float4[]), %(count)s)", tuple(values))
    else:
        cur.executemany("INSERT INTO " + index_config.get_value('coarse_table_name') + " (id, vector, count) VALUES (%(id)s, %(vector)s, %(count)s)", tuple(values))
    con.commit()
    return

def add_to_database(words, cq, codebook, pq_quantization, coarse_counts, fine_counts, con, cur, index_config, batch_size, logger):
    # add codebook
    add_codebook_to_database(codebook, fine_counts, con, cur, index_config)

    # add coarse quantization
    add_cq_to_database(cq, coarse_counts, con, cur, index_config)

    # add fine qunatization
    values = []
    for i in range(len(pq_quantization)):
        output_vec = utils.serialize_vector(pq_quantization[i][1])
        # print('pq_quantization[i]', pq_quantization[i])
        values.append({"coarse_tag": str(pq_quantization[i][0]*len(cq)+pq_quantization[i][2]), "word_id": (i // len(cq))+1, "vector": output_vec})
        # print(values[0])
        if (i % (batch_size-1) == 0) or (i == (len(pq_quantization)-1)):
            if USE_BYTEA_TYPE:
                cur.executemany("INSERT INTO "+ index_config.get_value('fine_table_name') + " (coarse_tag, word_id,vector) VALUES (%(coarse_tag)s, %(word_id)s, vec_to_bytea(%(vector)s::int2[]))", tuple(values))
            else:
                cur.executemany("INSERT INTO "+ index_config.get_value('fine_table_name') + " (coarse_tag, word_id,vector) VALUES (%(coarse_tag)s, %(word_id)s, %(vector)s)", tuple(values))
            con.commit()
            logger.log(Logger.INFO, 'Inserted ' +  str(i+1) + ' vectors')
            values = []
    return

def main(argc, argv):
    db_config = Configuration('config/db_config.json')
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
    words, vectors, vectors_size = utils.get_vectors(index_config.get_value('vec_file_path'), logger, max_count=2000)
    logger.log(logger.INFO, 'vectors_size :' + str(vectors_size))

    # determine coarse quantizer
    cq = None
    if index_config.has_key('coarse_quantizer_file'):
        cq_filename = index_config.get_value('coarse_quantizer_file')
        if cq_filename:
            logger.log(Logger.INFO, 'Use coarse quantizer from ' + cq_filename)
            cq = qcreator.load_quantizer(cq_filename)
    if type(cq) == type(None):
        logger.log(Logger.INFO, 'Create new coarse quantizer')
        # create coarse quantizer
        cq = qcreator.create_coarse_quantizer(vectors[:train_size_coarse], centr_num_coarse)
        print('length...', [np.linalg.norm(x) for x in cq])
        # store coarse quantizer
        qcreator.store_quantizer(cq, 'coarse_quantizer.pcl')

    # determine codebook
    codebook = None
    if index_config.has_key('residual_codebook_file'):
        codebook_filename = index_config.get_value('residual_codebook_file')
        if codebook_filename:
            logger.log(Logger.INFO, 'Use residual codebook from ' + codebook_filename)
            codebook = qcreator.load_quantizer(codebook_filename)
    if type(codebook) == type(None):
        logger.log(Logger.INFO, 'Create new residual codebook')
        # calculate codebook based on residuals
        codebook = create_fine_quantizer(cq, vectors[:train_size_fine], m, k, logger) # TODO adapt training to multiple coarse quantization case
        # store codebook
        qcreator.store_quantizer(codebook, 'residual_codebook.pcl')

    con = None
    cur = None

    # create db connection
    try:
        con = psycopg2.connect("dbname='" + db_config.get_value('db_name') + "' user='" + db_config.get_value('username') + "' host='" + db_config.get_value('host') + "' password='" + db_config.get_value('password') + "'")
    except:
        logger.log(logger.ERROR, 'Can not connect to database')
        return
    cur = con.cursor()

    utils.init_tables(con, cur, get_table_information(index_config), logger)
    utils.disable_triggers(index_config.get_value('fine_table_name'),con, cur)

    # create index with quantizers
    logger.log(logger.INFO, 'Start index creation (single cycle)')
    start = time.time()
    index, coarse_counts, fine_counts = create_index_with_faiss(vectors[:vectors_size], cq, codebook, logger)
    end = time.time()
    logger.log(logger.INFO, 'Finish index creation after ' + str(end - start) + ' seconds')
    # add to database
    add_to_database(words, cq, codebook, index, coarse_counts, fine_counts, con, cur, index_config, batch_size, logger)
    logger.log(logger.INFO, 'Create database index structures')
    utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_word_index_name'), 'word_id', con, cur, logger)
    utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_coarse_index_name'), 'coarse_tag', con, cur, logger)
    utils.enable_triggers(index_config.get_value('fine_table_name'), con, cur)

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
