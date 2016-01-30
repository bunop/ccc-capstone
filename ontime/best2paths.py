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
   
Your mission (should you choose to accept it!) is to find, for each X-Y-Z and 
day/month (dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) 
that satisfy constraints (a) and (b) and have the best individual performance with 
respect to constraint (c), if such flights exist.

"""

## Imports
from collections import namedtuple
from datetime import datetime, time
from operator import itemgetter, add
from pyspark import SparkConf, SparkContext

# add pyspark cassandra
import pyspark_cassandra

## Module Constants
APP_NAME = "Top 10 carriers in decreasing order of on-time arrival performance at Y from X."

# My functions
from common import *

# a named tuple for flat path like this:
fields = ('flightdate1', 'origin1', 'dest1', 'flightnum1', 'crsdeptime1', 'crsarrtime1', 'arrdelay1', 'flightdate2', 'origin2', 'dest2', 'flightnum2', 'crsdeptime2', 'crsarrtime2', 'arrdelay2')

# A namedtuple object
TwoPaths = namedtuple('TwoPaths', fields)

def flatpath(line):
    """Flat a path 2 path tuple"""
    
    # get the two path tuples
    path1, path2 = line
    
    # convert into list
    path1, path2 = list(path1), list(path2)
    
    # merge a path in a single record
    record = path1 + path2
    
    # return a tuple
    return TwoPaths(*record)
    
def getBest(group, element):
    """Add and element to the list, order the list and get the top of the list"""
    
    group = add([group], [element])
    
    # each element in a group is a [airlineid, depdelay]
    group.sort(key=itemgetter(6))
    
    # remove all elements after the 10 index. When reducing, two lists could be evaluated
    if len(group) > 1:
        group = group[0]
        
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
    FlightData = arrived_data.map(lambda m: (m.FlightDate, m.Origin, m.Dest, m.FlightNum, m.CRSDepTime, m.CRSArrTime, m.ArrDelay))
    
    # I need to filter data two times. Tom wants his flights scheduled to depart airport X before 12:00 PM local time
    path1 = FlightData.filter(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): crsdeptime < time(hour=12, minute=00))
    path2 = FlightData.filter(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): crsdeptime > time(hour=12, minute=00))
    
    # Tom wants to arrive at each destination with as little delay as possible (Clarification 1/24/16: assume you know the actual delay of each flight).
    # order by arr delay and then get only the first element
    path1ByFlight = path1.keyBy(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): (flightdate, origin, dest))
    path2ByFlight = path2.keyBy(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): (flightdate, origin, dest))
    
    # reduce the flight on the same day on the same path. Get best arrival performance
    reducedPath1 = path1ByFlight.reduceByKey(getBest).values()
    reducedPath2 = path2ByFlight.reduceByKey(getBest).values()
    
    # I can traform path by Origin and destination key, in order to join path1 destionation with path2 origin
    destPath1 = reducedPath1.keyBy(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): dest)
    originPath2 = reducedPath2.keyBy(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): origin)
    
    # Now I can do a join with the two path. Airport Y (path1 dest, pat2 origin) is the key
    joinedPath = destPath1.join(originPath2).values().map(flatpath)
    
    # The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). 
    # For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008. A difference between
    # two datetime days is a datetime.timedelta
    twoDaysPath = joinedPath.filter(lambda x: (x.flightdate2 - x.flightdate1).days == 2)

    # Store values in Cassandra database (flightnum1 INT, origin1 TEXT, dest1 TEXT, departure1 TIMESTAMP, arrival1 TIMESTAMP, arrdelay1 FLOAT, flightnum2 INT, origin2 TEXT, dest2 TEXT, departure2 TIMESTAMP, arrival2 TIMESTAMP, arrdelay2 FLOAT)
    best2path = twoDaysPath.map(lambda x: {"flightnum1": x.flightnum1, "origin1": x.origin1, "dest1": x.dest1, "departure1": sumDateTime(x.flightdate1, x.crsdeptime1), "arrival1": sumDateTime(x.flightdate1, x.crsarrtime1), "arrdelay1": x.arrdelay1, "flightnum2": x.flightnum2, "origin2": x.origin2, "dest2": x.dest2, "departure2": sumDateTime(x.flightdate2, x.crsdeptime2), "arrival2": sumDateTime(x.flightdate2, x.crsarrtime2), "arrdelay2": x.arrdelay2})
    
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
