# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 22:41:41 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Common functions for apacke spark

"""

import os
import csv

from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import itemgetter, add

## Module Constants
HDFS_PREFIX = "hdfs://ip-172-31-25-15.eu-central-1.compute.internal:8020"
LOOKUP_DIR = os.path.join(HDFS_PREFIX, "/user/paolo/capstone/lookup/")
DATA_DIR = os.path.join(HDFS_PREFIX, "/user/paolo/capstone/airline_ontime/filtered_data.gz/")
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

# Those are my fields
fields = ("FlightDate", "AirlineID", "FlightNum", "Origin", "OriginCityName", "OriginStateName", "Dest", "DestCityName", "DestStateName", "CRSDepTime",  "DepDelay", "CRSArrTime", "ArrDelay", "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance")

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
    row[fields.index("FlightNum")] = int(row[fields.index("FlightNum")])
    
    try:
        if row[fields.index("CRSDepTime")] == "2400":
            row[fields.index("CRSDepTime")] = "0000"
            
        row[fields.index("CRSDepTime")] = datetime.strptime(row[fields.index("CRSDepTime")], TIME_FMT).time()
    except ValueError:
        raise Exception, "problem in evaluating %s" %(row[fields.index("CRSArrTime")])
        
    try:
        if row[fields.index("CRSArrTime")] == "2400":
            row[fields.index("CRSArrTime")] = "0000"
            
        row[fields.index("CRSArrTime")] = datetime.strptime(row[fields.index("CRSArrTime")], TIME_FMT).time()
        
    except ValueError:
        raise Exception, "problem in evaluating %s" %(row[fields.index("CRSArrTime")])
        
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
