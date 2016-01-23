
## Spark Application - execute with spark-submit

## Imports
import csv

from StringIO import StringIO
from collections import namedtuple
from pyspark import SparkConf, SparkContext

## Module Constants
APP_NAME = "Top 10 airlines by on-time arrival performance"
LOOKUP_DIR = "hdfs://sandbox.hortonworks.com:8020/user/paolo/capstone/lookup/"


# Those are my fields
fields = ("FlightDate", "AirlineID", "Origin", "OriginCityName", "OriginStateName", "Dest", "DestCityName", "DestStateName", "CRSDepTime",  "DepDelay", "CRSArrTime", "ArrDelay", "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance")

# A namedtuple object
Ontime = namedtuple('Ontime', fields)

def split(line):
    """ Operator function for splitting a line with csv module"""
    reader = csv.reader(StringIO(line))
    return reader.next()

def parse(row):
    """
    Parses a row and returns a named tuple.
    """

    return Ontime(*row)

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
    ontime = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/paolo/capstone/airline_ontime/filtered_data/").map(split).map(parse)

    #get airline with no delay
    ontime.filter(lambda x: float(x.ArrDelay) <= 0 ).collect()[0:10]

#main function
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster("local[*]")
    conf = conf.setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
