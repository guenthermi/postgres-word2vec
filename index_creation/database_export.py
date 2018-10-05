import psycopg2
import index_utils as utils

USE_BYTEA_TYPE = True

def create_connection(db_config, logger):
    con = None
    cur = None
    print("dbname='" + db_config.get_value('db_name') + "' user='" + db_config.get_value('username') + "' host='" + db_config.get_value('host') + "' password='" + db_config.get_value('password') + "'")
    # create db connection
    try:
        con = psycopg2.connect("dbname='" + db_config.get_value('db_name') + "' user='" + db_config.get_value('username') + "' host='" + db_config.get_value('host') + "' password='" + db_config.get_value('password') + "'")
    except:
        logger.log(logger.ERROR, 'Can not connect to database')
        return
    cur = con.cursor()
    return con, cur

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

def add_multi_cq_to_database(cq, coarse_counts, con, cur, index_config):
    BATCH_SIZE = 100
    m = len(cq)
    num_centr = index_config.get_value('k_coarse')

    # add quantizer
    for pos in range(len(cq)):
        values = []
        for i in range(len(cq[pos])):
            output_vec = utils.serialize_vector(cq[pos][i])
            values.append({"pos": pos, "code": i, "vector": output_vec})
        if USE_BYTEA_TYPE:
            cur.executemany("INSERT INTO "+ index_config.get_value('coarse_table_name') + " (pos,code,vector) VALUES (%(pos)s, %(code)s, vec_to_bytea(%(vector)s::float4[]))", tuple(values))
        else:
            cur.executemany("INSERT INTO "+ index_config.get_value('coarse_table_name') + " (pos,code,vector) VALUES (%(pos)s, %(code)s, %(vector)s)", tuple(values))
        con.commit()

    # add counts
    divide_code = lambda code, units, length: tuple([int((code / units**i) % units) for i in range(length)]) # devides code into centroid ids
    batch = []
    for code in range(num_centr**m):
        key = divide_code(code, num_centr, m)
        count = coarse_counts[key] if key in coarse_counts else 0
        batch.append({"id": code, "count": count})
        if code % BATCH_SIZE == 0:
            cur.executemany("INSERT INTO " + index_config.get_value('coarse_table_name') + "_counts" + " (id, count) VALUES (%(id)s, %(count)s)", tuple(batch))
            con.commit()
            batch = []
    cur.executemany("INSERT INTO " + index_config.get_value('coarse_table_name') + "_counts" + " (id, count) VALUES (%(id)s, %(count)s)", tuple(batch))
    con.commit()
    return
