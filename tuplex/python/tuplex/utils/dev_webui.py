import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

from common import *

find_or_start_mongodb('localhost', 27071, 'tuplex-history', 'mongodb.log')

# now it's time to do the same thing for the WebUI (and also check it's version v.s. the current one!)


print('hello world')