import logging
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.info('testing mongodb init')

from tuplex.utils.common import find_or_start_mongodb

res = find_or_start_mongodb('localhost', 27017, './webui/data', './webui/mongod.log')
