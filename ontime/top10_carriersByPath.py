## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created mar 26 gen 2016, 20.49.06, CET

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

For each source-destination pair X-Y, rank the top-10 carriers in decreasing order 
of on-time arrival performance at Y from X.

"""

from __future__ import print_function

## Imports
import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# add pyspark cassandra, and streaming
# http://katychuang.me/blog/2015-09-30-kafka_spark.html
import pyspark_cassandra
from pyspark_cassandra import streaming

## Module Constants
CHECKPOINT_DIR = "checkpoint2/top10_carriersByPath"
APP_NAME = "Top 10 carriers in decreasing order of on-time arrival performance at Y from X."

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
    ssc = StreamingContext(sc, 30)
    
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
    to get only 10 elements"""
    
    group = add(group, element)
    
    # each element in a group is a [airlineid, depdelay]
    group.sort(key=itemgetter(1))
    
    # remove all elements after the 10 index. When reducing, two lists could be evaluated
    if len(group) > 10:
        group = group[:10]
        
    return group

def addIndex(group):
    for i, line in enumerate(group):
        line.insert(0,i+1)
        
    return group
    
def main(kvs):
    """Main function"""

    # get this spark context
    global ssc
    sc = ssc.sparkContext
    
    # Load the airlines lookup dictionary
    airlines = dict(sc.textFile(os.path.join(LOOKUP_DIR,"Lookup_AirlineID.csv" )).map(splitOne).collect())

    # Broadcast the lookup dictionary to the cluster. Broadcast variables allow the programmer
    # to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
    airline_lookup = sc.broadcast(airlines)

    # Get lines from kafka stream
    ontime_data = kvs.map(lambda x: x[1]).map(split).flatMap(parse)

    # filter out cancelled or diverted data: http://spark.apache.org/examples.html
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False and x.ArrDelay is not None)

    # map by Airport origin, Air port destination and airline id key
    CarrierData = arrived_data.map(lambda m: ((m.Origin, m.Dest,  m.AirlineID), m.ArrDelay))
    
    # calculate average with mapreduceaverage: Trasorm each value in a list
    collectDelays = CarrierData.updateStateByKey(updateFunction)
    
    # calculate average with mapreduce mapreduce average: Trasorm each value in a list
    averageByKey = collectDelays.reduceByKey(add).map(lambda (key, values): (key, sum(values)/float(len(values))))

    # traforming data using Origin, Dest as a key, and (AirlineID, ArrDelay) as value
    PathData = averageByKey.map(lambda ((origin, dest, airlineid), arrdelay): ((origin, dest), [[airlineid, arrdelay]]))
    
    # reducing data by Origin. Keep best top 10 performances
    reducedPath = PathData.reduceByKey(getTop10).mapValues(addIndex)
    
    # transform the rdd in a flatten rdd by kesy
    top10Paths = reducedPath.flatMapValues(lambda x: x)
    
    # print the top 10 delays
    top10Paths.pprint()

    # Store values in Cassandra database
    carriersByPath = top10Paths.map(lambda ((origin, dest), (rank, airlineid, arrdelay)): {"rank":rank, "origin":origin, "destination":dest, "airlineid":airlineid, "airline":airline_lookup.value[str(airlineid)], "arrdelay":arrdelay})
    
    # Use LOWER characters
    carriersByPath.saveToCassandra("capstone","carriersbypath")


#main function
if __name__ == "__main__":
    # Configure Spark. Create a new context or restore from checkpoint
    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)
    
    # get this spark context
    sc = ssc.sparkContext
    
    # http://stackoverflow.com/questions/24686474/shipping-python-modules-in-pyspark-to-other-nodes
    sc.addPyFile("common.py")

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
    kvs = KafkaUtils.createStream(ssc, ZKQUORUM, "top10_carriersByPath", {TOPIC: 1}, kafkaParams={ 'auto.offset.reset': 'smallest'})
    
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
            
    

