#!/usr/bin/python3

import sys
import psycopg2
import time
import numpy as np

import index_utils as utils

STD_USER = 'postgres'
STD_PASSWORD = 'postgres'
STD_HOST = 'localhost'

BATCH_SIZE = 50000

VEC_TABLE_NAME = 'google_vecs_norm'
VEC_INDEX_NAME = 'google_vecs_norm_word_idx'

VEC_FILE_PATH = '../vectors/google_vecs.txt'

NORMALIZED = True

def init_tables(con, cur):
    # drop old table
    query_clear = "DROP TABLE IF EXISTS " + VEC_TABLE_NAME + ";"
    result = cur.execute(query_clear)
    con.commit()
    print('Exexuted DROP TABLE on ', VEC_TABLE_NAME)

    # create table
    query_create_table = "CREATE TABLE " + VEC_TABLE_NAME + " (id serial PRIMARY KEY, word varchar(100), vector float4[]);"
    result = cur.execute(cur.mogrify(query_create_table, (VEC_TABLE_NAME,)))
    # commit changes
    con.commit()
    print('Created new table', VEC_TABLE_NAME)

    return

def serialize_array(array):
    output = '{'
    for elem in array:
        try:
            val = float(elem)
        except:
            # print('Could not serialize', elem, 'in', array)
            return None
        output += str(elem) + ','
    return output[:-1] + '}'

def serialize_as_norm_array(array):
    vector = []
    for elem in array:
        try:
            vector.append(float(elem))
        except:
            # print('Could not serialize', elem, 'in', array)
            return None
    output = '{'
    length = np.linalg.norm(vector)
    for elem in vector:
        output += str(elem / length) + ','
    return output[:-1] + '}'

def insert_vectors(filename, con, cur):
    f = open(filename)
    (_, size) = f.readline().split()
    count = 1
    line = f.readline()
    values = []
    while line:
        splits = line.split()
        vector = None
        if NORMALIZED:
            vector = serialize_as_norm_array(splits[1:])
        else:
            vector = serialize_array(splits[1:])
        if (len(splits[0]) < 100) and (vector != None):
            values.append({"word": splits[0], "vector": vector})
        if count % BATCH_SIZE == 0:
            cur.executemany("INSERT INTO "+ VEC_TABLE_NAME + " (word,vector) VALUES (%(word)s, %(vector)s)", tuple(values))
            con.commit()
            print('Inserted', count, 'vectors')
            values = []

        count+= 1
        line = f.readline()

    cur.executemany("INSERT INTO "+ VEC_TABLE_NAME + " (word,vector) VALUES (%(word)s, %(vector)s)", tuple(values))
    con.commit()
    print('Inserted', count, 'vectors')
    values = []

    return

def main(argc, argv):
    global VEC_TABLE_NAME
    global VEC_INDEX_NAME
    global NORMALIZED

    if argc < 3:
        print('Please provide the database name and filename')
        return
    db_name = argv[1]
    filename = argv[2]

    if argc > 3:
        VEC_TABLE_NAME = argv[3]
    if argc > 4:
        VEC_INDEX_NAME = argv[4]
    if argc > 5:
        NORMALIZED = (int(argv[5]) == 1)

    user = STD_USER
    password = STD_PASSWORD
    host = STD_HOST
    # init db connection
    try:
        con = psycopg2.connect("dbname='" + db_name + "' user='" + user + "' host='" + host + "' password='" + password + "'")
    except:
        print('Can not connect to database')
        return

    cur = con.cursor()

    init_tables(con, cur)

    insert_vectors(filename, con, cur)

    # commit changes
    con.commit()

    # create index
    utils.create_index(VEC_TABLE_NAME, VEC_INDEX_NAME, 'word', con, cur)

    # close connection
    con.close()

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
