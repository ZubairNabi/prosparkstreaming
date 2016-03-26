from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from sys import argv, exit
try: import simplejson as json
except ImportError: import json

if len(argv) != 5:
    print 'Usage: yelp_pyspark.py <appname> <batchInterval> <hostname> <port>'
    exit(-1)

appname = argv[1]
batch_interval = int(argv[2])
hostname = argv[3]
port = int(argv[4])

sc = SparkContext(appName=appname)
ssc = StreamingContext(sc, batch_interval)

records = ssc.socketTextStream(hostname, port)
json_records = records.map(lambda rec: json.loads(rec))
restaurant_records = json_records.filter(lambda rec: 'attributes' in rec and 'Wi-Fi' in rec['attributes'])
wifi_pairs = restaurant_records.map(lambda rec: (rec['attributes']['Wi-Fi'], rec['stars']))
wifi_counts = wifi_pairs.combineByKey(lambda v: (v, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg_stars = wifi_counts.map(lambda (key, (sum_, count)): (key, sum_ / count))
avg_stars.pprint()

ssc.start()
ssc.awaitTermination()
