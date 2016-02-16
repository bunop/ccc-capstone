## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 12 11:43:14 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Top 10 Airports (ex 1.1)

Remove checkpoint with:

$ hadoop fs -rm -r -skipTrash /user/ec2-user/checkpoint/top10_airports

"""

from __future__ import print_function

import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Global variables
CHECKPOINT_DIR = "checkpoint/top10_airports"
OUTPUT_DIR = "intermediate/top10_airports/"
APP_NAME = "Top 10 Airports"

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
    ssc = StreamingContext(sc, 60)
    
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

def main(kvs):
    """Main function"""
    
    # Get lines from kafka stream
    ontime_data = kvs.map(lambda x: x[1]).map(split).flatMap(parse)
    
    # Get origin and destionation
    origin = ontime_data.map(lambda x: (x.Origin,1)).reduceByKey(lambda a, b: a+b)
    dest = ontime_data.map(lambda x: (x.Dest,1)).reduceByKey(lambda a, b: a+b)
    
    # Union of the twd RDD. Sum by the same key. Then remember it
    popular = origin.union(dest).reduceByKey(lambda a, b: a+b)
    
    # traforming data using 1 as a key, and (AirlineID, ArrDelay) as value
    popular2 = popular.map(lambda (airport, count): (True, [(airport, count)]))
    
    # Flat map values
    airports = popular2.flatMapValues(lambda x: x).map(lambda (key, value): value)
    
    # debug
    airports.pprint()
    
    # Saving data in hdfs
    airports.repartition(1).saveAsTextFiles(OUTPUT_DIR)
    

#main function
if __name__ == "__main__":
    # Configure Spark. Create a new context or restore from checkpoint
    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, functionToCreateContext)
    
    # get this spark context
    sc = ssc.sparkContext
    
    # http://stackoverflow.com/questions/24686474/shipping-python-modules-in-pyspark-to-other-nodes
    sc.addPyFile("common.py")

    # Create a Transformed DStream. Read Kafka from first offset. To get a list of
    # kafka parameters: http://kafka.apache.org/08/configuration.html
    kvs = KafkaUtils.createStream(ssc, ZKQUORUM, "top10_airports", {TOPIC: 1}, kafkaParams={ 'auto.offset.reset': 'smallest'})
    
    # Execute Main functionality
    main(kvs)
    
    # start stream
    ssc.start()
    ssc.awaitTermination()
    
            
            
    
