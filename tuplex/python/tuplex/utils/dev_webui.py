import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

from common import *

find_or_start_mongodb('localhost', 27017, 'tuplex-history', 'mongodb.log')

mongo_uri = mongodb_uri('localhost', 27017)

# now it's time to do the same thing for the WebUI (and also check it's version v.s. the current one!)
version_info = find_or_start_webui(mongo_uri, 'localhost', 5000, 'gunicorn.log')

logging.info('Found WebUI: {}'.format(version_info))