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
    
    # calculate ontime average: http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/.
    # create a map like (label, (sum, count)).
    sumCount = ArrDelay.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))
    
    # calculating average
    averageByKey = sumCount.map(lambda (label, (value_sum, count)): (label, value_sum / count))
    
    # getting data from RDD
    averageByKey = averageByKey.collectAsMap()
    
    # sort by average values:http://stackoverflow.com/questions/613183/sort-a-python-dictionary-by-value
    sorted_delays = sorted(averageByKey.items(), key=itemgetter(1))
    
    # print the top 10 delays
    for item in sorted_delays[:10]:
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
