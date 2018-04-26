import sys
import psycopg2

from logger import *
from config import *
import pq_index
import ivfadc
import pq_index as pq
import index_manager as im
import index_utils as utils

HELP_TEXT = '\033[1mload_index.py\033[0m index_file index_type index_config_path'

def add_to_database(db_config, index_config, type, index_file, logger):

    # create db connection
    con = None
    try:
        con = psycopg2.connect("dbname='" + db_config.get_value('db_name') + "' user='" + db_config.get_value('username') + "' host='" + db_config.get_value('host') + "' password='" + db_config.get_value('password') + "'")
    except:
        logger.log(Logger.ERROR, 'Can not connect to database')
        return
    cur = con.cursor()

    if type == 'pq':
        data = im.load_index(index_file)
        utils.init_tables(con, cur, pq_index.get_table_information(index_config), logger)
        pq_index.add_to_database(data['words'], data['codebook'], data['index'], data['counts'], con, cur, index_config, db_config.get_value('batch_size'), logger)

        utils.create_index(index_config.get_value("pq_table_name"), index_config.get_value("pq_index_name"), 'word', con, cur, logger)

    elif type == 'ivfadc':
        data = im.load_index(index_file)
        utils.init_tables(con, cur, ivfadc.get_table_information(index_config), logger)

        ivfadc.add_to_database(data['words'], data['cq'], data['codebook'], data['index'], data['coarse_counts'], data['fine_counts'], con, cur, index_config, db_config.get_value('batch_size'), logger)

        utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_word_index_name'), 'word', con, cur, logger)
        utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_coarse_index_name'), 'coarse_id', con, cur, logger)
    elif type == 'ivfadc_pipeline':
        # TODO test
        data = im.load_pipeline_ivfadc_index(index_file, index_file + '.tmp', index_config.get_value('coarse_quantizer_file'), index_config.get_value('residual_codebook_file'))
        utils.init_tables(con, cur, ivfadc.get_table_information(index_config), logger)

        ivfadc.add_to_database(data['words'], data['cq'], data['codebook'], data['index'], data['coarse_counts'], data['fine_counts'], con, cur, index_config, db_config.get_value('batch_size'), logger)

        utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_word_index_name'), 'word', con, cur, logger)
        utils.create_index(index_config.get_value('coarse_table_name'), index_config.get_value('fine_coarse_index_name'), 'coarse_id', con, cur, logger)
    elif type == 'pq_pipeline':
        # TODO test
        data = im.load_pipeline_pq_index(index_file, index_file + '.tmp', index_config.get_value('codebook_file'))
        utils.init_tables(con, cur, pq.get_table_information(index_config), logger)

        pq.add_to_database(data['words'], data['codebook'], data['index'], data['counts'], con, cur, index_config, db_config.get_value('batch_size'), logger)

        utils.create_index(index_config.get_value("pq_table_name"), index_config.get_value("pq_index_name"), 'word', con, cur, logger)
    else:
        logger.log(logger.WARNING, 'Index type ' + str(type) + ' unknown')
    return


def main(argc, argv):
    db_config = Configuration('config/db_config.json')
    logger = Logger(db_config.get_value('log'))
    index_type = None
    index_config = None
    if argc < 4:
        print(HELP_TEXT)
        return
    else:
        index_file = argv[1]
        index_type = argv[2]
        index_config = Configuration(argv[3])
    add_to_database(db_config, index_config, index_type, index_file, logger)
    # logger.log(Logger.ERROR, 'Configuration file for index creation required')


if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
