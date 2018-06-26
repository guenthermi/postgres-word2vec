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
