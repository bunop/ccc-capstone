## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 12 11:43:14 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Top 10 Airports (ex 1.1)

Remove checkpoint with:

$ hadoop fs -rm -r -skipTrash /user/paolo/checkpoint2/top10_airports

"""

from __future__ import print_function

import sys

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Global variables
CHECKPOINT_DIR = "checkpoint2/top10_airports"
APP_NAME = "Top 10 Airports"
TOPIC = "test"

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
    ssc = StreamingContext(sc, 2)
    
    # set checkpoint directory
    ssc.checkpoint(CHECKPOINT_DIR)
    
    # return streaming spark context
    return ssc

def updateFunction(newValues, oldValues):
    # for the first time, initialize with newValues
    if oldValues is None:
       return sum(newValues)
      
    # add the new values with the previous running count to get the new count
    return sum(newValues, oldValues)

def getTop10(group, element):
    """Add and element to the list, order the list and then filter the lower value
    to get onlt 10 elements"""
    
    group = add(group, element)
    
    # each element in a group is a [airlineid, depdelay]. Reverse true, since I want more counts
    group.sort(key=itemgetter(1), reverse=True)
    
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
    
    # Get lines from kafka stream
    ontime_data = kvs.map(lambda x: x[1]).map(split).map(parse)
    
    # Get origin and destionation
    origin = ontime_data.map(lambda x: (x.Origin,1)).reduceByKey(lambda a, b: a+b)#.updateStateByKey(updateFunction)
    dest = ontime_data.map(lambda x: (x.Dest,1)).reduceByKey(lambda a, b: a+b)#.updateStateByKey(updateFunction)
    
    # Union of the twd RDD. Sum by the same key. Then remember it
    popular = origin.union(dest).reduceByKey(lambda a, b: a+b).updateStateByKey(updateFunction)
    
    # traforming data using 1 as a key, and (AirlineID, ArrDelay) as value
    popular2 = popular.map(lambda (airport, count): (True, [(airport, count)]))

    # reducing data by 1. Keep best top 10 performances
    top10 = popular2.reduceByKey(getTop10)
    
    # Flat map values
    top10Airport = top10.flatMapValues(lambda x: x).map(lambda (key, value): value)
    
    # print the top 10 delays
    top10Airport.pprint()
    
    # Call a function on each RDD of this DStream
    #top10Airport.foreachRDD(get_output)
    

#main function
if __name__ == "__main__":
    # Configure Spark. Create a new context or restore from checkpoint
    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)

    # Create a Transformed DStream. Read Kafka from first offset
    kvs = KafkaUtils.createStream(ssc, ZKQUORUM, "spark-streaming-consumer", {TOPIC: 1}, kafkaParams={ 'auto.offset.reset': 'smallest'})
    
    # Execute Main functionality
    main(kvs)
    
    
    # start stream
    ssc.start()
    ssc.awaitTermination()
    