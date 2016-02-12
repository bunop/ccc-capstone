## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on lun 25 gen 2016, 16.30.05, CET

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

For each airport X, rank the top-10 carriers in decreasing order of on-time departure
performance from X. I will need to compute the results for ALL input values (e.g.,
airport X, source-destination pair X-Y, etc.) for which the result is nonempty.
These results should then be stored in Cassandra so that the results for an input
value can be queried by a user.

"""

## Imports
import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# add pyspark cassandra
import pyspark_cassandra

## Module Constants
CHECKPOINT_DIR = "checkpoint2/top10_airlines"
APP_NAME = "top-10 carriers in decreasing order of on-time departure performance from X"
TOPIC = "test"

## Ranges
#offsetRanges = []

## my functions
from common import *

# Function to create and setup a new StreamingContext
def functionToCreateContext():
    # new context
    conf = SparkConf()
    conf = conf.setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)
    
    # http://stackoverflow.com/questions/24686474/shipping-python-modules-in-pyspark-to-other-nodes
    sc.addPyFile("common.py")
    
    # As argument Spark Context and batch retention
    ssc = StreamingContext(sc, 1)
    
    # set checkpoint directory
    ssc.checkpoint(CHECKPOINT_DIR)
    
    # return streaming spark context
    return ssc

def updateFunction(newValues, oldValues):
    # for the first time, initialize with newValues
    if oldValues is None:
       return newValues
      
    # add the new values with the previous running count to get the new count
    return add(newValues, oldValues)

def getTop10(group, element):
    """Add and element to the list, order the list and then filter the lower value
    to get onlt 10 elements"""
    
    group = add(group, element)
    
    # each element in a group is a [airlineid, depdelay]
    group.sort(key=itemgetter(1))
    
    # remove all elements after the 10 index. When reducing, two lists could be evaluated
    if len(group) > 10:
        group = group[:10]
        
    return group

# A generic function. Could I call cassandra here?
def get_output(rdd):
    rdd_data = rdd.collect()
    
    if len(rdd_data) == 0:
        return
    
    for item in rdd_data:
        print(item)

#def storeOffsetRanges(rdd):
#    global offsetRanges
#    offsetRanges = rdd.offsetRanges()
#    return rdd
#
#def printOffsetRanges(rdd):
#    for o in offsetRanges:
#        print("%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset))

def main(kvs):
    """Main function"""

    # get this spark context
    global ssc
    sc = ssc.sparkContext
    
    # Load the airlines lookup dictionary
    airlines = dict(sc.textFile(os.path.join(LOOKUP_DIR,"Lookup_AirlineID.csv" )).map(split).collect())

    # Broadcast the lookup dictionary to the cluster. Broadcast variables allow the programmer
    # to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
    airline_lookup = sc.broadcast(airlines)

    # Get lines from kafka stream
    ontime_data = kvs.map(lambda x: x[1]).map(split).map(parse)
    
    # Call a function on each RDD of this DStream
#    ontime_data.foreachRDD(printOffsetRanges)
    
    # filter out cancelled or diverted data: http://spark.apache.org/examples.html
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False and x.AirlineID is not None and x.DepDelay is not None)

    # map by Airport, Carrier key
    CarrierData = arrived_data.map(lambda m: ((m.Origin, m.AirlineID), m.DepDelay))
    
    # calculate average with mapreduce mapreduce average: Trasorm each value in a list
    collectDelays = CarrierData.map(lambda (key, value): (key, value)).updateStateByKey(updateFunction)
    
    # calculate average with mapreduce mapreduce average: Trasorm each value in a list
    averageByKey = collectDelays.map(lambda (key, values): (key, sum(values)/float(len(values))))

    # traforming data using Origin as a key, and (AirilineID, DepDelay) as value
    OriginData = averageByKey.map(lambda ((origin, airlineid), depdelay): (origin, [(airlineid, depdelay)]))
    
    # reducing data by Origin. Keep best top 10 performances
    reducedOrigin = OriginData.reduceByKey(getTop10)
    
    # transform the rdd in a flatten rdd by kesy
    top10Origin = reducedOrigin.flatMapValues(lambda x: x)
    
    # print the top 10 delays
    top10Origin.pprint()

    # Store values in Cassandra database
    #carriersByAirport = top10Origin.map(lambda (origin, (airlineid, depdelay)): {"origin":origin, "airlineid":airlineid, "airline":airline_lookup.value[str(airlineid)], "depdelay":depdelay})
    
    # Use LOWER characters
    #carriersByAirport.saveToCassandra("capstone","carriersbyairport")


#main function
if __name__ == "__main__":
    # Configure Spark. Create a new context or restore from checkpoint
    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)

    # Create a Transformed DStream. Read Kafka from first offset
    # creating a stream
    # :param ssc:  StreamingContext object
    # :param zkQuorum:  Zookeeper quorum (hostname:port,hostname:port,..).
    # :param groupId:  The group id for this consumer.
    # :param topics:  Dict of (topic_name -> numPartitions) to consume.
    #                 Each partition is consumed in its own thread.
    # :param kafkaParams: Additional params for Kafka
    # :param storageLevel:  RDD storage level.
    # :param keyDecoder:  A function used to decode key (default is utf8_decoder)
    # :param valueDecoder:  A function used to decode value (default is utf8_decoder)
    # :return: A DStream object
    kvs = KafkaUtils.createStream(ssc, ZKQUORUM, "spark-streaming-consumer", {TOPIC: 1}, kafkaParams={ 'auto.offset.reset': 'smallest'})
    
    # Execute Main functionality
    main(kvs)
    
    # start stream
    ssc.start()
    
    #ssc.awaitTermination()
    while True:
        try:
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("Shutting down Spark...")
            ssc.stop(stopSparkContext=True, stopGraceFully=True)

