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
import index_utils as utils
import index_manager as im
import quantizer_creation as qcreator
from pq_index_creator import *

USE_PIPELINE_APPROACH = True
USE_BYTEA_TYPE = True

def get_table_information(index_config):
    if USE_BYTEA_TYPE:
        return ((index_config.get_value("pq_table_name"),"(id serial PRIMARY KEY, word varchar(100), vector bytea)"),
            (index_config.get_value("cb_table_name"), "(id serial PRIMARY KEY, pos int, code int, vector bytea, count int)"))
    else:
        return ((index_config.get_value("pq_table_name"),"(id serial PRIMARY KEY, word varchar(100), vector int[])"),
            (index_config.get_value("cb_table_name"), "(id serial PRIMARY KEY, pos int, code int, vector float4[], count int)"))

def create_index_with_faiss(vectors, codebook, logger):
    logger.log(Logger.INFO, 'Length of vectors: ' + str(len(vectors)))
    result = []
    indices = []
    m = len(codebook)
    len_centr = int(len(vectors[0]) / m)
    # create indices for codebook
    for i in range(m):
        index = faiss.IndexFlatL2(len_centr)
        logger.log(Logger.INFO, str(codebook[i])) # TODO replace info
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
            logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
    logger.log(Logger.INFO, 'Appended ' + str(len(result)) + ' vectors')
    return result

def create_index(vectors, codebook, logger):
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
            logger.log(Logger.INFO, 'Appended ' + str(count) + ' vectors')
    return result

def add_to_database(words, codebook, pq_quantization, counts, con, cur, index_config, batch_size, logger):
    logger.log(Logger.INFO, 'Length of words: ' + str(len(words)) + ' Length of pq_quantization: ' + str(len(pq_quantization)))
    # add codebook
    add_codebook_to_database(codebook, counts, con, cur, index_config)

    # add pq qunatization
    values = []
    for i in range(len(pq_quantization)):
        output_vec = utils.serialize_vector(pq_quantization[i])
        values.append({"word": words[i][:100], "vector": output_vec})
        if (i % (batch_size-1) == 0) or (i == (len(pq_quantization)-1)):
            if USE_BYTEA_TYPE:
                cur.executemany("INSERT INTO "+ index_config.get_value("pq_table_name") + " (word,vector) VALUES (%(word)s, vec_to_bytea(%(vector)s::int2[]))", tuple(values))
            else:
                cur.executemany("INSERT INTO "+ index_config.get_value("pq_table_name") + " (word,vector) VALUES (%(word)s, %(vector)s)", tuple(values))
            con.commit()
            logger.log(Logger.INFO, 'Inserted ' + str(i+1) + ' vectors')
            values = []
    return

def add_codebook_to_database(codebook, counts, con, cur, index_config):
    for pos in range(len(codebook)):
        values = []
        for i in range(len(codebook[pos])):
            output_vec = utils.serialize_vector(codebook[pos][i])
            values.append({"pos": pos, "code": i, "vector": output_vec, "count": counts[(pos, i)]})
        if USE_BYTEA_TYPE:
            cur.executemany("INSERT INTO "+ index_config.get_value("cb_table_name") + " (pos,code,vector, count) VALUES (%(pos)s, %(code)s, vec_to_bytea(%(vector)s::float4[]), %(count)s)", tuple(values))
        else:
            cur.executemany("INSERT INTO "+ index_config.get_value("cb_table_name") + " (pos,code,vector, count) VALUES (%(pos)s, %(code)s, %(vector)s, %(count)s)", tuple(values))
        con.commit()
    return

def add_batch_to_database(word_batch, pq_quantization, con, cur, index_config, batch_size, logger):
    values = []
    for i in range(len(pq_quantization)):
        output_vec = utils.serialize_vector(pq_quantization[i])
        values.append({"word": word_batch[i][:100], "vector": output_vec})
        if (i % (batch_size-1) == 0) or (i == (len(pq_quantization)-1)):
            if USE_BYTEA_TYPE:
                cur.executemany("INSERT INTO "+ index_config.get_value("pq_table_name") + " (word,vector) VALUES (%(word)s, vec_to_bytea(%(vector)s::int2[]))", tuple(values))
            else:
                cur.executemany("INSERT INTO "+ index_config.get_value("pq_table_name") + " (word,vector) VALUES (%(word)s, %(vector)s)", tuple(values))
            con.commit()
            values = []
    return

def determine_counts(codebook, pq_quantization):
    result = dict()
    for i in range(len(pq_quantization)):
        for j in range(len(pq_quantization[i])):
            pos = j
            code = pq_quantization[i][j]
            if not (pos, code) in result:
                result[(pos, code)] = 1
            else:
                result[(pos, code)] += 1
    return result

def main(argc, argv):
    db_config = Configuration('config/db_config.json')
    logger = Logger(db_config.get_value('log'))
    if argc < 2:
        logger.log(Logger.ERROR, 'Configuration file for index creation required')
        return
    index_config = Configuration(argv[1])

    batch_size = db_config.get_value("batch_size")

    # get vectors
    words, vectors, vectors_size = utils.get_vectors(index_config.get_value("vec_file_path"), logger)
    logger.log(Logger.INFO, 'vectors_size : ' + str(vectors_size))

    # determine codebook
    codebook = None
    if index_config.has_key('codebook_file'):
        codebook_filename = index_config.get_value('codebook_file')
        if codebook_filename:
            logger.log(Logger.INFO, 'Use codebook from ' + codebook_filename)
            codebook = qcreator.load_quantizer(codebook_filename)
    if type(codebook) == type(None):
        logger.log(Logger.INFO, 'Create new codebook')
        # apply k-means -> get codebook
        codebook = qcreator.create_quantizer(vectors[:index_config.get_value('train_size')], index_config.get_value('m'), index_config.get_value('k'), logger)
        # save codebook to file (optional)
        qcreator.store_quantizer(codebook, 'codebook.pcl')

    con = None
    cur = None
    if (index_config.get_value('add_to_database')):
        # create db connection
        try:
            con = psycopg2.connect("dbname='" +  db_config.get_value('db_name') + "' user='" +  db_config.get_value('username') + "' host='" +  db_config.get_value('host') + "' password='" +  db_config.get_value('password') + "'")
        except:
            logger.log(Logger.ERROR, 'Can not connect to database')
            return
        cur = con.cursor()

        utils.init_tables(con, cur, get_table_information(index_config), logger)
        utils.disable_triggers(index_config.get_value("pq_table_name"), con,cur)


    # create index with qunatizer
    use_pipeline = False
    if index_config.has_key('pipeline'):
        use_pipeline = index_config.get_value('pipeline')

    # singel cycle
    if not use_pipeline:
        logger.log(logger.INFO, 'Start index creation (single cycle)')
        start = time.time()
        index = create_index_with_faiss(vectors[:vectors_size], codebook, logger)
        end = time.time()
        logger.log(Logger.INFO, 'Finish index creation after ' + str(end - start) + ' seconds')

        counts = determine_counts(codebook, index)

        # add to file
        if (index_config.get_value('export_filename')):
            index_data = dict({
                'words': words,
                'codebook': codebook,
                'index': index,
                'counts': counts
            })
            im.save_index(index_data, index_config.get_value('export_filename'))

        if (index_config.get_value('add_to_database')):
            add_to_database(words, codebook, index, counts, con, cur, index_config, batch_size, logger)
            logger.log(logger.INFO, 'Create database index structures')
            utils.create_index(index_config.get_value("pq_table_name"), index_config.get_value("pq_index_name"), 'word', con, cur, logger)
            utils.enable_triggers(index_config.get_value("pq_table_name"), con, cur)

    # pipeline approach
    if use_pipeline:
        logger.log(logger.INFO, 'Start index creation (pipeline)')
        start = time.time()
        feeder = VectorFeeder(vectors[:vectors_size], words)
        m = len(codebook)
        len_centr = int(len(vectors[0]) / m)
        calculation = PQIndexCreator(codebook, m, len_centr, logger)
        counts = dict()
        output_file = None
        if (index_config.get_value('export_pipeline_data')):
            output_file = open(index_config.get_value('export_pipeline_data'), 'wb')
        while (feeder.has_next()):
            # calculate
            batch, word_batch = feeder.get_next_batch(batch_size)
            entries, counts = calculation.index_batch(batch)
            # write to database or add to file
            if (index_config.get_value('add_to_database')):
                add_batch_to_database(word_batch, entries, con, cur, index_config, batch_size, logger)
                logger.log(logger.INFO, 'Added ' + str(feeder.get_cursor() - batch_size + len(batch)) + ' vectors to the database')
            if (index_config.get_value('export_pipeline_data')):
                index_batch = dict({
                    'words': word_batch,
                    'index': entries,
                })
                pickle.dump(index_batch, output_file)
                f = open(index_config.get_value('export_pipeline_data')+'.tmp', 'wb')
                pickle.dump(counts, f)
                f.close()
                logger.log(logger.INFO, 'Processed ' + str(feeder.get_cursor() - batch_size + len(batch)) + ' vectors')
        if output_file:
            output_file.close()
        if (index_config.get_value('add_to_database')):
            # add codebook to database
            add_codebook_to_database(codebook, counts, con, cur, index_config)
            logger.log(Logger.INFO, 'Added codebook entries into database')
            logger.log(logger.INFO, 'Create database index structures')
            utils.create_index(index_config.get_value("pq_table_name"), index_config.get_value("pq_index_name"), 'word', con, cur, logger)
            utils.enable_triggers(index_config.get_value("pq_table_name"), con, cur)

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
