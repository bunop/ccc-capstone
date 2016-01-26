## Spark Application - execute with spark-submit
# -*- coding: utf-8 -*-
"""
Created on lun 25 gen 2016, 16.30.05, CET

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

For each airport X, rank the top-10 carriers in decreasing order of on-time departure
performance from X. I will need to compute the results for ALL input values (e.g.,
airport X, source-destination pair X-Y, etc.) for which the result is nonempty.
These results should then be stored in Cassandra so that the results for an input
value can be queried by a user.

"""

## Imports
import os
import csv

from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import itemgetter, add
from pyspark import SparkConf, SparkContext

# add pyspark cassandra
import pyspark_cassandra

## Module Constants
APP_NAME = "Top 10 airlines by on-time arrival performance"
LOOKUP_DIR = "hdfs://sandbox.hortonworks.com:8020/user/paolo/capstone/lookup/"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

# Those are my fields
fields = ("FlightDate", "AirlineID", "Origin", "OriginCityName", "OriginStateName", "Dest", "DestCityName", "DestStateName", "CRSDepTime",  "DepDelay", "CRSArrTime", "ArrDelay", "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance")

# A namedtuple object
Ontime = namedtuple('Ontime', fields)

def split(line):
    """Operator function for splitting a line with csv module"""
    reader = csv.reader(StringIO(line))
    return reader.next()

def parse(row):
    """Parses a row and returns a named tuple"""

    row[fields.index("FlightDate")] = datetime.strptime(row[fields.index("FlightDate")], DATE_FMT).date()
    row[fields.index("AirlineID")] = int(row[fields.index("AirlineID")])
    row[fields.index("CRSDepTime")] = datetime.strptime(row[fields.index("CRSDepTime")], TIME_FMT).time()
    row[fields.index("CRSArrTime")] = datetime.strptime(row[fields.index("CRSArrTime")], TIME_FMT).time()
    row[fields.index("Cancelled")] = bool(int(row[fields.index("Cancelled")]))
    row[fields.index("Diverted")] = bool(int(row[fields.index("Diverted")]))

    # handle cancellation code
    if row[fields.index("CancellationCode")] == '"':
        row[fields.index("CancellationCode")] = None

    #`handle float values
    for index in ["DepDelay", "ArrDelay", "CRSElapsedTime", "Distance", "ActualElapsedTime", "AirTime"]:
        try:
            row[fields.index(index)] = float(row[fields.index(index)])
        except ValueError:
            row[fields.index(index)] = None

    return Ontime(*row)

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
    airlines = dict(sc.textFile(os.path.join(LOOKUP_DIR,"Lookup_AirlineID.csv" )).map(split).collect())

    # Broadcast the lookup dictionary to the cluster. Broadcast variables allow the programmer
    # to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
    airline_lookup = sc.broadcast(airlines)

    # Read the CSV Data into an RDD (data are stored on HFDS)
    # The HDSF location in specified in core-site.xml (grep fs /etc/hadoop/conf/core-site.xml)
    # http://stackoverflow.com/questions/27478096/cannot-read-a-file-from-hdfs-using-spark
    ontime_data = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/paolo/capstone/airline_ontime/filtered_data/").map(split).map(parse)

    # filter out cancelled or diverted data: http://spark.apache.org/examples.html
    arrived_data = ontime_data.filter(lambda x: x.Cancelled is False and x.Diverted is False)

    # map by Airport, Carrier key
    CarrierData = arrived_data.map(lambda m: ((m.Origin, m.AirlineID), m.DepDelay))
    
    # calculate average with mapreduce mapreduce average: Trasorm each value in a list
    averageByKey = CarrierData.map(lambda (key, value): (key, [value])).reduceByKey(add).map(lambda (key, values): (key, sum(values)/float(len(values))))

    # traforming data using Origin as a key, and (AirilineID, DepDelay) as value
    OriginData = averageByKey.map(lambda ((origin, airlineid), depdelay): (origin, [(airlineid, depdelay)]))
    
    # reducing data by Origin. Keep best top 10 performances
    reducedOrigin = OriginData.reduceByKey(getTop10)
    
    # transform the rdd in a flatten rdd by kesy
    top10Origin = reducedOrigin.flatMapValues(lambda x: x)

    # Store values in Cassandra database
    carriersByAirport = top10Origin.map(lambda (origin, (airlineid, depdelay)): {"origin":origin, "airlineid":airlineid, "airline":airline_lookup.value[str(airlineid)], "depdelay":depdelay})
    
    # Use LOWER characters
    carriersByAirport.saveToCassandra("capstone","carriersbyairport")


#main function
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster("local[*]")
    conf = conf.setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)