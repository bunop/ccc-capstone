## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 18:13:00 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>
"""

from __future__ import print_function
from ast import literal_eval

## Imports
import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import add

# Global variables
CHECKPOINT_DIR = "checkpoint2/top10_airlines.2"
APP_NAME = "Top 10 Airlines 2"

## my functions
from common import *

# override default TOPIC
TOPIC = "top10_airlines"

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
      
    # return a list of elements
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

def calcAverage(values):
    """calculate average element by element"""
    
    num = [value[0] for value in values]
    den = [value[1] for value in values]
    
    return float(sum(num))/sum(den)

def main(kvs):
    """Main function"""
    
    # get data from kafka: (k, (sum(v), len(v)))
    data = kvs.map(lambda x: literal_eval(x[1]))
    
    # collect delays from stream
    collectDelays = data.updateStateByKey(updateFunction)
    
    # calculate average
    averageByKey = collectDelays.map(lambda (key, values): (key, calcAverage(values)))
    
    # # traforming data using 1 as a key, and (AirlineID, ArrDelay) as value
    #AirlineIDData = averageByKey.map(lambda (airlineid, arrdelay): (True, [(airlineid, arrdelay)]))
    AirlineIDData = averageByKey.map(lambda (key, value): (True, [(key, value)]))

    # reducing data by 1. Keep best top 10 performances
    reducedAirlineID = AirlineIDData.reduceByKey(getTop10)
    
    # transform the rdd in a flatten rdd by kesy
    top10Airline = reducedAirlineID.flatMapValues(lambda x: x).map(lambda (key, ((airline_id, airline), delay)): (airline_id, airline, delay))
    
    # print the top 10 delays
    top10Airline.pprint()
    
    
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
