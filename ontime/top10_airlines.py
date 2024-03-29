## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 18:13:00 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Top 10 Airlines (ex 1.2)

Remove checkpoint with:

$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/top10_airlines

"""

from __future__ import print_function

## Imports
import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Global variables
CHECKPOINT_DIR = "checkpoint2/top10_airlines"
OUTPUT_DIR = "intermediate/top10_airlines/"
APP_NAME = "Top 10 Airlines"

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
    ssc = StreamingContext(sc, 10)
    
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
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False and x.AirlineID is not None and x.ArrDelay is not None)
    
    # map by arrived delays
    ArrDelay = arrived_data.map(lambda m: ((m.AirlineID, airline_lookup.value[str(m.AirlineID)]), m.ArrDelay))
    
    # sum elements and number of elements, to store them in a intermediate file (k, (sum(v), len(v)))
    collectDelays = ArrDelay.map(lambda (key, value): (key, [value])).reduceByKey(add).map(lambda (key, values): (key, [sum(values), len(values)]))
    
    #debug
    collectDelays.pprint()
    
    # Saving data in hdfs
    collectDelays.saveAsTextFiles(OUTPUT_DIR)
    

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
    kvs = KafkaUtils.createStream(ssc, ZKQUORUM, "top10_airline", {TOPIC: 1}, kafkaParams={ 'auto.offset.reset': 'smallest'})
    
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
