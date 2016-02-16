# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 16:27:11 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

Writing a kafka producer

"""

import os
import sys
import time
import logging
import argparse

from kafka.common import LeaderNotAvailableError, UnknownError, KafkaTimeoutError
from kafka import KafkaProducer

# Pydoop
import pydoop.hdfs as hdfs

from common import HOST

# a Global variable
producer = None

#An useful way to defined a logger lever, handler, and formatter
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(os.path.basename(sys.argv[0]))

def print_response(record_metadata=None):
    if record_metadata:
        msg = ['Topic: {0}'.format(record_metadata.topic)]
        msg += ['Partition: {0}'.format(record_metadata.partition)]
        msg += ['Offset: {0}'.format(record_metadata.offset)]
        msg = "; ".join(msg)
        logger.debug(msg)

def submitChunk(topic, data, trials=3, async=True):
    step = 0
    global producer
    
    while step < trials:
        step += 1
        try:
            future = producer.send(topic, "\n".join(data))
            
            if async is False:
                record_metadata = future.get(timeout=10)
                print_response(record_metadata)
            
            return #if submitted
            
        except (LeaderNotAvailableError, UnknownError, KafkaTimeoutError), message:
            logger.error(message)
            # https://github.com/mumrah/kafka-python/issues/249
            time.sleep(10)
            
    #If arrive here
    logger.warn("data %s ignored" %(data))
    return #anyway

def processChunk(myfile, topic):
    with hdfs.open(myfile["name"]) as handle:
        data = []
        
        for i, line in enumerate(handle):
            #strip line
            line = line.strip()
            data += [line]
            
            if i % 5000 == 0:
                #Submit data (my function)
                submitChunk(topic, data, trials=3)
                data = []
            
            if i % 20000 == 0 and i != 0:
                logger.info("%s lines submitted for %s" %(i, myfile["name"]))
                
        #for every line
        #submit the rest of the data
        submitChunk(topic, data, trials=3)
        data = []

def submitLine(topic, line, trials=3, async=True):
    step = 0
    global producer
    
    while step < trials:
        step += 1
        try:
            future = producer.send(topic, line)
            
            if async is False:
                record_metadata = future.get(timeout=10)
                print_response(record_metadata)
            
            return #if submitted
            
        except (LeaderNotAvailableError, UnknownError, KafkaTimeoutError), message:
            logger.error(message)
            # https://github.com/mumrah/kafka-python/issues/249
            time.sleep(10)
            
    #If arrive here
    logger.warn("line %s ignored" %(line))
    return #anyway

def processLine(myfile, topic):
    with hdfs.open(myfile["name"]) as handle:
        for i, line in enumerate(handle):
            #strip line
            line = line.strip()
            
            #Submit data (my function)
            submitLine(topic, line, trials=3)
            
            if i % 20000 == 0 and i != 0:
                logger.info("%s lines submitted for %s" %(i, myfile["name"]))
                
        #for every line

def main(directory, topic, byline):
    #get a hdfs object
    myHdfs = hdfs.hdfs()
    myPath = myHdfs.walk(directory)
    
    # a global variable
    global producer 

    # Get a producer object
    producer = KafkaProducer(bootstrap_servers=["node4:6667"], compression_type='gzip', acks=1, retries=2)
    
    for myfile in myPath:
        #Skip directory recursive
        if myfile["kind"] == "directory":
            logger.debug("ignoring %s" %(myfile))
            continue
        
        elif myfile["kind"] == "file":
            pass
        
        else:
            raise Exception, "Unknown kind %s for %s" %(myfile["kind"], myfile["name"])
            
        #Skip name in particoular
        if "_SUCCESS" in myfile["name"] or "_temporary" in myfile["name"]:
            logger.debug("ignoring %s" %(myfile))
            continue
        
        #Skip 0 size files
        if myfile["size"] == 0:
            logger.debug("ignoring %s" %(myfile))
            continue
        
        logger.info("Working on %s" %(myfile["name"]))

        #call processChunk if I want to submit chunk
        if byline is False:
            processChunk(myfile, topic)
            
        else:
            #Otherwise submit line by line
            processLine(myfile, topic)

        #with file open
        logger.info("Completed %s" %(myfile["name"]))
        
        #sleep some time
        time.sleep(1)
                    
    # for all files in HDFS
    producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan a directory and upload data in kafka-topic')
    parser.add_argument('-d', '--directory', type=str, required=True, help='The HDFS directory file')
    parser.add_argument('-t', '--topic', type=str, required=True, help="The kafka topic")
    parser.add_argument('--line', action='store_true', default=False, help="submit line by line")
    args = parser.parse_args()
        
    #call main function
    main(directory=args.directory, topic=args.topic, byline=args.line)

