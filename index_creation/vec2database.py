#!/usr/bin/python3

import sys
import psycopg2
import time
import numpy as np

from config import *
from logger import *
import index_utils as utils

USE_BYTEA_TYPE = True


def init_tables(con, cur, table_name, logger):
    # drop old table
    query_clear = "DROP TABLE IF EXISTS " + table_name + ";"
    result = cur.execute(query_clear)
    con.commit()
    logger.log(Logger.INFO, 'Executed DROP TABLE on ' + table_name)

    # create table
    query_create_table = None
    if USE_BYTEA_TYPE:
        query_create_table = "CREATE TABLE " + table_name + " (id serial PRIMARY KEY, word varchar(100), vector bytea);"
    else:
        query_create_table = "CREATE TABLE " + table_name + " (id serial PRIMARY KEY, word varchar(100), vector float4[]);"
    result = cur.execute(cur.mogrify(query_create_table, (table_name, )))
    # commit changes
    con.commit()
    logger.log(Logger.INFO, 'Created new table ' + table_name)

    return


def serialize_array(array):
    output = '{'
    for elem in array:
        try:
            val = float(elem)
        except:
            return None
        output += str(elem) + ','
    return output[:-1] + '}'


def serialize_as_norm_array(array):
    vector = []
    for elem in array:
        try:
            vector.append(float(elem))
        except:
            return None
    output = '{'
    length = np.linalg.norm(vector)
    for elem in vector:
        output += str(elem / length) + ','
    return output[:-1] + '}'


def insert_vectors(filename, con, cur, table_name, batch_size, normalized,
                   logger):
    f = open(filename)
    (_, size) = f.readline().split()
    d = int(size)
    count = 1
    line = f.readline()
    values = []
    while line:
        splits = line.split(' ')
        vector = None
        if normalized:
            vector = serialize_as_norm_array(splits[1:])
        else:
            vector = serialize_array(splits[1:])
        if (len(splits[0]) < 100) and (vector !=
                                       None) and (len(splits) == (d + 1)):
            values.append({"word": splits[0], "vector": vector})
        else:
            logger.log(Logger.WARNING, 'parsing problem with ' + line)
            count -= 1
        if count % batch_size == 0:
            if USE_BYTEA_TYPE:
                cur.executemany(
                    "INSERT INTO " + table_name +
                    " (word,vector) VALUES (%(word)s, vec_to_bytea(%(vector)s::float4[]))",
                    tuple(values))
            else:
                cur.executemany("INSERT INTO " + table_name +
                                " (word,vector) VALUES (%(word)s, %(vector)s)",
                                tuple(values))
            con.commit()
            logger.log(Logger.INFO, 'Inserted ' + str(count - 1) + ' vectors')
            values = []

        count += 1
        line = f.readline()

    if USE_BYTEA_TYPE:
        cur.executemany(
            "INSERT INTO " + table_name +
            " (word,vector) VALUES (%(word)s, vec_to_bytea(%(vector)s::float4[]))",
            tuple(values))
    else:
        cur.executemany("INSERT INTO " + table_name +
                        " (word,vector) VALUES (%(word)s, %(vector)s)",
                        tuple(values))
    con.commit()
    logger.log(Logger.INFO, 'Inserted ' + str(count - 1) + ' vectors')
    values = []

    return


def main(argc, argv):

    if argc > 2:
        db_config = Configuration(argv[2])
    else:
        db_config = Configuration('config/db_config.json')

    logger = Logger(db_config.get_value('log'))

    if argc < 2:
        logger.log(Logger.ERROR,
                   'Configuration file for index creation required')
        return
    vec_config = Configuration(argv[1])

    user = db_config.get_value('username')
    password = db_config.get_value('password')
    host = db_config.get_value('host')
    db_name = db_config.get_value('db_name')

    # init db connection
    try:
        con = psycopg2.connect(
            "dbname='" + db_name + "' user='" + user + "' host='" + host +
            "' password='" + password + "'")
    except:
        logger.log(Logger.ERROR, 'Can not connect to database')
        return

    cur = con.cursor()

    init_tables(con, cur, vec_config.get_value('table_name'), logger)

    insert_vectors(
        vec_config.get_value('vec_file_path'), con, cur,
        vec_config.get_value('table_name'), db_config.get_value('batch_size'),
        vec_config.get_value('normalized'), logger)

    # commit changes
    con.commit()

    # create index
    utils.create_index(
        vec_config.get_value('table_name'), vec_config.get_value('index_name'),
        'word', con, cur, logger)

    # close connection
    con.close()


if __name__ == "__main__":
    main(len(sys.argv), sys.argv)
