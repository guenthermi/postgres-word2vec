#!/usr/bin/python3

import sys
import psycopg2
import time
import numpy as np

STD_USER = 'postgres'
STD_PASSWORD = 'postgres'
STD_HOST = 'localhost'

BATCH_SIZE = 50000

VEC_TABLE_NAME = 'google_vecs_norm'
VEC_FILE_PATH = '../vectors/google_vecs.txt'

NORMALIZED = True

def init_tables(con, cur):
    # check if table already exists
    query_check_existence = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=%s;"
    cur.execute(cur.mogrify(query_check_existence, (VEC_TABLE_NAME,)))
    rows = cur.fetchall()
    print(rows)
    table_exists = (len(rows) > 0)

    # create table
    if not table_exists:
        query_create_table = "CREATE TABLE " + VEC_TABLE_NAME + " (id serial PRIMARY KEY, word varchar(100), vector float4[]);"
        result = cur.execute(cur.mogrify(query_create_table, (VEC_TABLE_NAME,)))
        # commit changes
        con.commit()
        print('Created new table', VEC_TABLE_NAME)
    else:
        # delete content of table
        query_clear = "DELETE FROM " + VEC_TABLE_NAME + ";"
        result = cur.execute(query_clear)
        con.commit()
        print('Table', VEC_TABLE_NAME, 'already exists. All entries are removed.')

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
    if argc < 2:
        print('Please provide the database name')
        return

    db_name = argv[1]
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

    insert_vectors(VEC_FILE_PATH, con, cur)

    # commit changes
    con.commit()

    # close connection
    con.close()

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
