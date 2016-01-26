## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 18:13:00 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Rank the days of the week by on-time arrival performance.

"""

## Imports
import os
import csv
import calendar

from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import itemgetter
from pyspark import SparkConf, SparkContext

## Module Constants
APP_NAME = "days of the week by on-time arrival performance"

## My functions
from common import *

# Others functions
def getDayOfWeek(date):
    """Get the day of the week from a datetime object"""

    #http://stackoverflow.com/questions/9847213/which-day-of-week-given-a-date-python
    return calendar.day_name[date.weekday()]

def main(sc):
    """Main function"""

    # Read the CSV Data into an RDD (data are stored on HFDS)
    # The HDSF location in specified in core-site.xml (grep fs /etc/hadoop/conf/core-site.xml)
    # http://stackoverflow.com/questions/27478096/cannot-read-a-file-from-hdfs-using-spark
    ontime_data = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/paolo/capstone/airline_ontime/filtered_data/").map(split).map(parse)

    # filter out cancelled or diverted data: http://spark.apache.org/examples.html
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False)

    # map delay by day of week
    ArrDelay = arrived_data.map(lambda m: (getDayOfWeek(m.FlightDate), m.ArrDelay))

    # calculate ontime average: http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/.
    # create a map like (label, (sum, count)).
    sumCount = ArrDelay.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # calculating average
    averageByKey = sumCount.map(lambda (label, (value_sum, count)): (label, value_sum / count))

    # getting data from RDD
    averageByKey = averageByKey.collectAsMap()

    # sort by average values:http://stackoverflow.com/questions/613183/sort-a-python-dictionary-by-value
    sorted_delays = sorted(averageByKey.items(), key=itemgetter(1))

    # Rank the days of the week by on-time arrival performance.
    for item in sorted_delays:
        print item

#main function
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster("local[*]")
    conf = conf.setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
