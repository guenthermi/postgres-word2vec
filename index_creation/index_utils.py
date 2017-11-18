#!/bin/python3

import numpy as np

PQ_TABLE_NAME = 'pq_quantization'
CODEBOOK_TABLE_NAME = 'pq_codebook'

# TABLE_INFORMATION = ((PQ_TABLE_NAME,"(id serial PRIMARY KEY, word varchar(100), vector int[])"),
#     (CODEBOOK_TABLE_NAME, "(id serial PRIMARY KEY, pos int, code int, vector float4[])"))


def get_vectors(filename, max_count=10**9, normalization=True):
    f = open(filename)
    line_splits = f.readline().split()
    size = int(line_splits[0])
    d = int(line_splits[1])
    words, vectors, count = [],np.zeros((size, d)).astype('float32'), 0
    print(count)
    print(line_splits)
    while (line_splits) and (count < max_count):
        line = f.readline()
        line_splits = line.split()
        if not line_splits:
            break
        word = line_splits[0]
        vector = [float(elem) for elem in line_splits[1:]]
        if normalization:
            v_len = np.linalg.norm(vector)
            vector = [x / v_len for x in vector]
        if len(vector) == 300:
            vectors[count] = vector
            words.append(word)
            count += 1
        else:
            print('Can not decode the following line: ', line);
        if count % 10000 == 0:
            print('INFO read', count, 'vectors')
    return words, vectors, count

def init_tables(con, cur, table_information):
    query_drop = "DROP TABLE IF EXISTS "
    for (name, schema) in table_information:
        query_drop += (" " + name + ",")
    query_drop = query_drop[:-1] + ";"
    result = cur.execute(query_drop)
    # commit drop
    con.commit()
    for (name, schema) in table_information:
        query_create_table = "CREATE TABLE " + name + " " + schema + ";"
        result = cur.execute(query_create_table)
        # commit changes
        con.commit()
        print('Created new table', name)
    return

def serialize_vector(vec):
    output_vec = '{'
    for elem in vec:
        output_vec += str(elem) + ','
    return output_vec[:-1] + '}'

def create_index(table_name, index_name, column_name, con, cur):
    query_create_index = "CREATE INDEX " + index_name + " ON " +  table_name + " (" + column_name + ");"
    cur.execute(query_create_index)
    con.commit()
