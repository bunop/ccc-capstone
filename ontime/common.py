# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 22:41:41 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Common functions for apacke spark

"""

import os
import csv
import datetime

from StringIO import StringIO
from collections import namedtuple
from operator import itemgetter, add

## Module Constants
TOPIC = "test"
HOST = "sandbox.hortonworks.com"
ZOOKEEPER = ["%s:2181" %(HOST)]
ZKQUORUM = ",".join(ZOOKEEPER) #zkQuorum:  Zookeeper quorum (hostname:port,hostname:port,..)
HDFS_PREFIX = "hdfs://%s:8020" %(HOST)
LOOKUP_DIR = os.path.join(HDFS_PREFIX, "/user/paolo/capstone/lookup/")
DATA_DIR = os.path.join(HDFS_PREFIX, "/user/paolo/capstone/airline_ontime/filtered_data")
TEST_DIR = os.path.join(HDFS_PREFIX, "/user/paolo/capstone/airline_ontime/test")
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

# Those are my fields
fields = ("FlightDate", "AirlineID", "FlightNum", "Origin", "OriginCityName", "OriginStateName", "Dest", "DestCityName", "DestStateName", "CRSDepTime",  "DepDelay", "CRSArrTime", "ArrDelay", "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance")

# A namedtuple object
Ontime = namedtuple('Ontime', fields)

def split(line):
    """Operator function for splitting a line with csv module"""
    reader = csv.reader(StringIO(line))
    return list(reader)

def splitOne(line):
    """Operator function for splitting a line with csv module"""
    reader = csv.reader(StringIO(line))
    return reader.next()

def parse(rows):
    """Parse multiple rows"""
    return [parse_row(row) for row in rows]
        

def parse_row(row):
    """Parses a row and returns a named tuple"""
    
    row[fields.index("FlightDate")] = datetime.datetime.strptime(row[fields.index("FlightDate")], DATE_FMT).date()
    row[fields.index("AirlineID")] = int(row[fields.index("AirlineID")])
    row[fields.index("FlightNum")] = int(row[fields.index("FlightNum")])
    
    #cicle amoung scheduled times    
    for index in ["CRSDepTime", "CRSArrTime"]:
        if row[fields.index(index)] == "2400":
            row[fields.index(index)] = "0000"
        
        # Handle time values
        try:
            row[fields.index(index)] = datetime.datetime.strptime(row[fields.index(index)], TIME_FMT).time()
                    
        except ValueError:
            #raise Exception, "problem in evaluating %s" %(row[fields.index(index)])
            row[fields.index(index)] = None
        
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
