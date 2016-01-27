## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created mar 26 gen 2016, 21.21.14, CET

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Tom wants to travel from airport X to airport Z. However, Tom also wants to stop 
at airport Y for some sightseeing on the way. More concretely, Tom has the following 
requirements: 
    
 * The second leg of the journey (flight Y-Z) must depart two days after the first 
   leg (flight X-Y). For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
 * Tom wants his flights scheduled to depart airport X before 12:00 PM local time 
   and to depart airport Y after 12:00 PM local time.
 * Tom wants to arrive at each destination with as little delay as possible 
   (Clarification 1/24/16: assume you know the actual delay of each flight).
   
Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy constraints (a) and (b) and have the best individual performance with respect to constraint (c), if such flights exist.

"""

## Imports
from datetime import datetime, time
from operator import itemgetter, add
from pyspark import SparkConf, SparkContext

# add pyspark cassandra
import pyspark_cassandra

## Module Constants
APP_NAME = "Top 10 carriers in decreasing order of on-time arrival performance at Y from X."

# My functions
from common import *

def sortByDelay(group, element):
    """Add and element to the list, and then order the list"""
    
    group = add(group, element)
    
    # each element in a group is a [(m.FlightNum, m.CRSDepTime, m.CRSArrTime, m.ArrDelay)]))
    group.sort(key=itemgetter(3))
        
    return group
    
def sumDateTime(date, time):
    """Sum time to a datetime object"""
    
    return datetime(year=date.year, month=date.month, day=date.day, hour=time.hour, minute=time.minute)

def main(sc):
    """Main function"""
    
    # Read the CSV Data into an RDD (data are stored on HFDS)
    # The HDSF location in specified in core-site.xml (grep fs /etc/hadoop/conf/core-site.xml)
    # http://stackoverflow.com/questions/27478096/cannot-read-a-file-from-hdfs-using-spark
    ontime_data = sc.textFile(DATA_DIR).map(split).map(parse)

    # filter out cancelled or diverted data: http://spark.apache.org/examples.html
    # consider to get only 2008 data as specified in requirements
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False and x.FlightDate.year == 2008)

    # I need to extract the values I need. I need the date, the departure and arrival time 
    # scheduled, and the delay
    FlightData = arrived_data.map(lambda m: ((m.FlightDate, m.Origin, m.Dest), [(m.FlightNum, m.CRSDepTime, m.CRSArrTime, m.ArrDelay)]))
    
    # I think that values mus be sorted by m.ArrDelay: Tom wants to arrive at each destination with as little delay as possible 
    # (Clarification 1/24/16: assume you know the actual delay of each flight).
    reducedData = FlightData.reduceByKey(sortByDelay)
    
    # transform the rdd in a flatten rdd by kesy
    sortedData = reducedData.flatMapValues(lambda x: x)
    
    # I need to filter data two times. Tom wants his flights scheduled to depart airport X before 12:00 PM local time
    path1 = sortedData.filter(lambda ((flightdate, origin, dest), (flightnum, crsdeptime, crsarrtime, arrdelay)): crsdeptime < time(hour=12, minute=00))
    path2 = sortedData.filter(lambda ((flightdate, origin, dest), (flightnum, crsdeptime, crsarrtime, arrdelay)): crsdeptime > time(hour=12, minute=00))
    
    # I can traform path by Origin and destination key, in order to join path1 destionation with path2 origin
    dest_path1 = path1.keyBy(lambda ((flightdate, origin, dest), (flightnum, crsdeptime, crsarrtime, arrdelay)): dest)
    origin_path2 = path2.keyBy(lambda ((flightdate, origin, dest), (flightnum, crsdeptime, crsarrtime, arrdelay)): origin)
    
    # Now I can do a join with the two path
    joined_path = dest_path1.join(origin_path2)
    

    # Store values in Cassandra database
    best2path = sortedData.map(lambda ((flightdate, origin, dest), (flightnum, crsdeptime, crsarrtime, arrdelay)): {"flightdate":flightdate, "origin":origin ,"destination":dest, "flightnum": flightnum, "crsdeptime": sumDateTime(flightdate, crsdeptime), "crsarrtime": sumDateTime(flightdate, crsarrtime), "arrdelay":arrdelay})
    
    # Use LOWER characters
    best2path.saveToCassandra("capstone","best2path")


#main function
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster("local[*]")
    conf = conf.setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
