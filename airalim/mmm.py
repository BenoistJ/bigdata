#import sys, os, json, datetime, logging
#from datetime import datetime

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf#
#
#reload(sys)#
#sys.setdefaultencoding('utf-8#')
#sys.path.append('/home/benois#t/user/python-libs')
#
#logger = logging.getLogger('main')
#logger.setLevel(logging.DEBUG)
#fh = logging.FileHandler('output.log', 'w')
#fh.setLevel(logging.DEBUG)
#ch = logging.StreamHandler()
#ch.setLevel(logging.DEBUG)
#formatter = logging.Formatter('%(asctime)s - %(message)s')
#fh.setFormatter(formatter)
#ch.setFormatter(formatter)
#logger.addHandler(fh)
#logger.addHandler(ch)

#batch_size = 100000;

# creation du contexte spark
conf = SparkConf()\
	.setAppName("Load test")\
	.set("spark.cassandra.connection.host", "192.168.1.100")\
	.set("spark.cassandra.output.batch.size.bytes", "5120")\
	.set("spark.cassandra.output.throughput_mb_per_sec", "4")
sc = CassandraSparkContext(conf=conf)

print sc.cassandraTable("test","kv").collect()

#start = datetime.now()
#
#items = []
#for num in range(0,batch_size):
#	items.append([num, num, num, num, num, num, num])
#
#rdd = sc.parallelize(items)
#rdd.saveToCassandra("z_app_di3_cass_keyspace", "bil_occup_totale_troncon")
#
#logger.info('Duration: {}'.format(datetime.now() - start))
#logger.info("\nFINISHED\n")
#