#!/bin/python3

import numpy as np
import psycopg2

from logger import *

def get_vectors(filename, logger, max_count=10**9, normalization=True):
    f = open(filename)
    line_splits = f.readline().split(' ')
    logger.log(Logger.INFO, str(line_splits))
    size = int(line_splits[0])
    d = int(line_splits[1])
    words, vectors, count = [],np.zeros((size, d), dtype='float32'), 0
    logger.log(Logger.INFO, str(count))
    logger.log(Logger.INFO, str(line_splits))
    while (line_splits != ['']) and (count < max_count):
        line = f.readline()
        line_splits = line.split(' ')
        if not line_splits:
            break
        word = line_splits[0]
        vector = []
        for elem in line_splits[1:]:
            try:
                vector.append(float(elem))
            except:
                break
        if normalization:
            v_len = np.linalg.norm(vector)
            vector = [x / v_len for x in vector]
        if (len(vector) == d) and (len(word) < 100):
            vectors[count] = vector
            words.append(word)
            count += 1
        else:
            logger.log(Logger.INFO, 'Can not decode the following line: ' + str(line));
        if count % 10000 == 0:
            logger.log(Logger.INFO, 'Read ' + str(count) + ' vectors')
    return words, vectors, count

def init_tables(con, cur, table_information, logger):
    query_drop = "DROP TABLE IF EXISTS "
    for (name, schema) in table_information:
        query_drop += (" " + name + ",")
    query_drop = query_drop[:-1] + " CASCADE;"
    result = cur.execute(query_drop)
    # commit drop
    con.commit()
    for (name, schema) in table_information:
        query_create_table = "CREATE TABLE " + name + " " + schema + ";"
        result = cur.execute(query_create_table)
        # commit changes
        con.commit()
        logger.log(Logger.INFO, 'Created new table ' + str(name))
    return

def serialize_vector(vec):
    output_vec = '{'
    for elem in vec:
        output_vec += str(elem) + ','
    return output_vec[:-1] + '}'

def disable_triggers(table_name, con, cur):
    cur.execute('ALTER TABLE ' + table_name + ' DISABLE trigger ALL;')
    con.commit();

def enable_triggers(table_name, con, cur):
    cur.execute('ALTER TABLE ' + table_name + ' ENABLE trigger ALL;')
    con.commit();

def create_index(table_name, index_name, column_name, con, cur, logger):
    query_drop = "DROP INDEX IF EXISTS " + index_name + ";"
    result = cur.execute(query_drop)
    con.commit()
    query_create_index = "CREATE INDEX " + index_name + " ON " +  table_name + " (" + column_name + ");"
    cur.execute(query_create_index)
    con.commit()
    logger.log(Logger.INFO, 'Created index ' + str(index_name) + ' on table ' + str(table_name) + ' for column ' + str(column_name))

def create_statistics_table(table_name, column_name, coarse_table_name, con, cur, logger):
    query = "SELECT create_statistics('" + table_name + "', '" + column_name + "', '" + coarse_table_name + "')"
    cur.execute(query)
    con.commit()
    logger.log(Logger.INFO, 'Created statistics table')
