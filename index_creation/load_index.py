import sys
import psycopg2

from logger import *
from config import *
import pq_index
import ivfadc
import index_manager as im
import index_utils as utils

HELP_TEXT = '\033[1mload_index.py\033[0m index_file index_type index_config_path'

def add_to_database(db_config, index_config, type, data, logger):

    # create db connection
    try:
        con = psycopg2.connect("dbname='" + db_config.get_value('db_name') + "' user='" + db_config.get_value('username') + "' host='" + db_config.get_value('host') + "' password='" + db_config.get_value('password') + "'")
    except:
        logger.log(Logger.ERROR, 'Can not connect to database')
        return
    cur = con.cursor()

    if type == 'pq':
        utils.init_tables(con, cur, pq_index.get_table_information(index_config), logger)
        pq_index.add_to_database(words, codebook, index, counts, con, cur, index_config, batch_size, logger)

        utils.create_index(index_config.get_value("pq_table_name"), index_config.get_value("pq_index_name"), 'word', con, cur, logger)

    elif type == 'ivfadc':
        utils.init_tables(con, cur, ivfadc.get_table_information(), logger)

        ivfadc.add_to_database(words, cq, codebook, index, coarse_counts, fine_counts, con, cur, batch_size)

        utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_word_index_name'), 'word', con, cur, logger)
        utils.create_index(index_config.get_value('fine_table_name'), index_config.get_value('fine_coarse_index_name'), 'coarse_id', con, cur, logger)
    else:
        logger.log(logger.WARNING, 'Index type ' + str(type) + ' unknown')
    return


def main(argc, argv):
    db_config = Configuration('db_config.json')
    logger = Logger(db_config.get_value('log'))
    index_type = None
    index_config = None
    if argc < 4:
        print HELP_TEXT
    else:
        index_file = argv[1]
        index_type = argv[2]
        index_config = Configuration(argv[3])
    data = im.load_index(index_file)
    add_to_database(db_config, index_config, index_type, data, logger)
    # logger.log(Logger.ERROR, 'Configuration file for index creation required')

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
