## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 18:13:00 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>
"""

## Imports
import os
import csv

from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import itemgetter
from pyspark import SparkConf, SparkContext

## Module Constants
APP_NAME = "Top 10 airlines by on-time arrival performance"

## my functions
from common import *

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

def main(sc):
    """Main function"""

    # Load the airlines lookup dictionary
    #airlines = dict(sc.textFile(os.path.join(LOOKUP_DIR,"Lookup_AirlineID.csv" )).map(split).collect())

    # Broadcast the lookup dictionary to the cluster. Broadcast variables allow the programmer
    # to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
    #airline_lookup = sc.broadcast(airlines)

    # Read the CSV Data into an RDD (data are stored on HFDS)
    # The HDSF location in specified in core-site.xml (grep fs /etc/hadoop/conf/core-site.xml)
    # http://stackoverflow.com/questions/27478096/cannot-read-a-file-from-hdfs-using-spark
    ontime_data = sc.textFile(DATA_DIR).map(split).map(parse)

    # filter out cancelled or diverted data: http://spark.apache.org/examples.html
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False and x.AirlineID is not None and x.ArrDelay is not None)
    
    # map by arrived delays
    #ArrDelay = arrived_data.map(lambda m: (airline_lookup.value[str(m.AirlineID)], m.ArrDelay))
    ArrDelay = arrived_data.map(lambda m: (m.AirlineID, m.ArrDelay))
    
    # calculate average with mapreduce mapreduce average: Trasorm each value in a list
    averageByKey = ArrDelay.map(lambda (key, value): (key, [value])).reduceByKey(add).map(lambda (key, values): (key, sum(values)/float(len(values))))

    # # traforming data using 1 as a key, and (AirlineID, ArrDelay) as value
    AirlineIDData = averageByKey.map(lambda (airlineid, arrdelay): (True, [(airlineid, arrdelay)]))

    # reducing data by 1. Keep best top 10 performances
    reducedAirlineID = AirlineIDData.reduceByKey(getTop10)
    
    # transform the rdd in a flatten rdd by kesy
    top10Airline = reducedAirlineID.flatMapValues(lambda x: x).map(lambda (key, value): value)
    
    # print the top 10 delays
    for item in top10Airline.collect():
        print item
    

    
#main function
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf()
    conf = conf.setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)
    
    # http://stackoverflow.com/questions/24686474/shipping-python-modules-in-pyspark-to-other-nodes
    sc.addPyFile("common.py")

    # Execute Main functionality
    main(sc)
