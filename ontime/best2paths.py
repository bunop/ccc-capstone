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
from datetime import datetime, time, timedelta
from operator import itemgetter, add
from pyspark import SparkConf, SparkContext

# add pyspark cassandra
import pyspark_cassandra

## Module Constants
APP_NAME = "Tom's best path"

# My functions
from common import *

# a named tuple for flat path like this:
fields = ('flightdate1', 'origin1', 'dest1', 'flightnum1', 'crsdeptime1', 'crsarrtime1', 'arrdelay1', 'flightdate2', 'origin2', 'dest2', 'flightnum2', 'crsdeptime2', 'crsarrtime2', 'arrdelay2', 'tot_delays')

# A namedtuple object
TwoPaths = namedtuple('TwoPaths', fields)

def filterByDays(line):
    """Filter a 2 path tuple (after join)"""
    
    # get the two path tuples
    path1, path2 = line
    
    # get the two flight date
    flightdate1, flightdate2 = path1[0], path2[0]
    
    return (flightdate2 - flightdate1).days == 2
        
def sumDateTime(date, time):
    """Sum time to a datetime object"""
    
    return datetime(year=date.year, month=date.month, day=date.day, hour=time.hour, minute=time.minute)

def getBest(el1, el2):
    """Add and element to the list, order the list and get the top of the list"""
    
    #this is the single element
    #(flightdate1, origin1, dest1, flightnum1, crsdeptime1, crsarrtime1, arrdelay1, flightdate2, origin2, dest2, flightnum2, crsdeptime2, crsarrtime2, arrdelay2, arrdelay1+arrdelay2)
    group = add(el1, el2)
    
    # a group is a list of tuples
    group.sort(key=itemgetter(14))
    
    # remove all elements after the 10 index. When reducing, two lists could be evaluated
    if len(group) > 1:
        group = [group[0]]
        
    return group

def main(sc):
    """Main function"""
    
    # Read the CSV Data into an RDD (data are stored on HFDS)
    # The HDSF location in specified in core-site.xml (grep fs /etc/hadoop/conf/core-site.xml)
    # http://stackoverflow.com/questions/27478096/cannot-read-a-file-from-hdfs-using-spark
    ontime_data = sc.textFile(DATA_DIR).map(split).map(parse)

    # filter out cancelled or diverted data: http://spark.apache.org/examples.html
    # consider to get only 2008 data as specified in requirements
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False and x.FlightDate.year == 2008 and x.CRSDepTime is not None and x.CRSArrTime is not None and x.ArrDelay is not None)

    # I need to extract the values I need. I need the date, the departure and arrival time 
    # scheduled, and the delay
    FlightData = arrived_data.map(lambda m: (m.FlightDate, m.Origin, m.Dest, m.FlightNum, m.CRSDepTime, m.CRSArrTime, m.ArrDelay))
    
    # I need to filter data two times. Tom wants his flights scheduled to depart airport X before 12:00 PM local time
    path1 = FlightData.filter(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): crsdeptime < time(hour=12, minute=00))
    path2 = FlightData.filter(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): crsdeptime > time(hour=12, minute=00))
    
    # filter path1 and path2 by locations
    #CMI → ORD → LAX, 04/03/2008
    #JAX → DFW → CRP, 09/09/2008
    #SLC → BFL → LAX, 01/04/2008
    #LAX → SFO → PHX, 12/07/2008
    #DFW → ORD → DFW, 10/06/2008
    #LAX → ORD → JFK, 01/01/2008
    #path1Filtered = path1.filter(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): origin in ('CMI', 'JAX', 'SLC', 'LAX', 'DFW', 'LAX') and dest in ('ORD', 'DFW', 'BFL', 'SFO', 'ORD', 'ORD'))
    #path2Filtered = path2.filter(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): origin in ('ORD', 'DFW', 'BFL', 'SFO', 'ORD', 'ORD') and dest in ('LAX', 'CRP', 'LAX', 'PHX', 'DFW', 'JFK'))        
    
    # I can traform path by Origin and destination key, in order to join path1 destionation with path2 origin
    # TIP: since the The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y).
    # i could subtrack 2 days from the second dataset; then join by two keys
    destPath1 = path1.keyBy(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): (dest, flightdate))
    
    # By subtracting two days from 2nd time, I could join by date
    originPath2 = path2.keyBy(lambda (flightdate, origin, dest, flightnum, crsdeptime, crsarrtime, arrdelay): (origin, flightdate-timedelta(2)))
    
    # Now I can do a join with the two path. Airport Y (path1 dest, pat2 origin) is the key. When a join is performed
    # RDD with the same keys are located on the same nodes. Calling a map to transform values could be ineficcinest since
    # the new keys will invalidate partitioning by keys. So, filter out dates before calling a new key map phase
    joinedPath = destPath1.join(originPath2).values()
    
    # Tom wants to arrive at each destination with as little delay as possible (Clarification 1/24/16: assume you know the actual delay of each flight)
    # I can sum delays for each 2 path, then order by such values. So
    twoDaysPathFlat = joinedPath.map(lambda ((flightdate1, origin1, dest1, flightnum1, crsdeptime1, crsarrtime1, arrdelay1), (flightdate2, origin2, dest2, flightnum2, crsdeptime2, crsarrtime2, arrdelay2)): ((flightdate1, flightdate2, origin1, dest1, dest2), [(flightdate1, origin1, dest1, flightnum1, crsdeptime1, crsarrtime1, arrdelay1, flightdate2, origin2, dest2, flightnum2, crsdeptime2, crsarrtime2, arrdelay2, arrdelay1+arrdelay2)]))
    
    # Tom wants to arrive at each destination with as little delay as possible (Clarification 1/24/16: assume you know the actual delay of each flight).
    # reducing data by sum of delays. keep only the best. First sum flight from the same dat with the same path
    # aggregatebykey first sum partition on the same node, them intra nodes
    twoDaysPathSorted = twoDaysPathFlat.aggregateByKey([], getBest, getBest)
    
    #transforming in a more easier object
    twoDaysPath = twoDaysPathSorted.values().map(lambda x: TwoPaths(*x[0]))
    
    # Store values in Cassandra database (flightnum1 INT, origin1 TEXT, dest1 TEXT, departure1 TIMESTAMP, arrival1 TIMESTAMP, arrdelay1 FLOAT, flightnum2 INT, origin2 TEXT, dest2 TEXT, departure2 TIMESTAMP, arrival2 TIMESTAMP, arrdelay2 FLOAT)
    best2path = twoDaysPath.map(lambda x: {"flightnum1": x.flightnum1, "origin1": x.origin1, "dest1": x.dest1, "departure1": sumDateTime(x.flightdate1, x.crsdeptime1), "arrival1": sumDateTime(x.flightdate1, x.crsarrtime1), "arrdelay1": x.arrdelay1, "flightnum2": x.flightnum2, "origin2": x.origin2, "dest2": x.dest2, "departure2": sumDateTime(x.flightdate2, x.crsdeptime2), "arrival2": sumDateTime(x.flightdate2, x.crsarrtime2), "arrdelay2": x.arrdelay2})
    
    # Use LOWER characters
    best2path.saveToCassandra("capstone","best2path")


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
    
