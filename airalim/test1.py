#!/usr/bin/python
import sys, os, json, datetime, logging
from datetime import datetime
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/home/benoist/user/python-libs')
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('output.log', 'w')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

batch_size = 10;

logger.info("---------------------------------------------------------------------------")
logger.info("CREATE CASSANDRA CONTEXT")
cluster = Cluster(["192.168.1.100"])
session = cluster.connect("z_app_di3_cass_keyspace")

def insert(consistency_level):
	logger.info("Inserting to Cassandra with consistency_level=" + str(consistency_level))
	start = datetime.now()
	query = SimpleStatement(
	    "INSERT INTO bil_occup_totale_troncon (troncon, cable, paire, niv_occ, voie, lien, atm) VALUES (%s, %s, %s, %s, %s, %s, %s)",
	    consistency_level=consistency_level)
	for i in range(1,100):
		for num in range(0,batch_size):
			x = str(num + (batch_size*i))
			session.execute(query, (x, x, x, x, x, x, x))
		logger.info(i*batch_size)
	logger.info('Duration: {}'.format(datetime.now() - start))

start = datetime.now()
insert(ConsistencyLevel.QUORUM)
#insert(ConsistencyLevel.ONE)

logger.info('Duration: {}'.format(datetime.now() - start))
logger.info("\nFINISHED\n")
