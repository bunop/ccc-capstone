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

def submit(topic, line, trials=3, async=True):
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

def main(directory, topic):
    #get a hdfs object
    myHdfs = hdfs.hdfs()
    myPath = myHdfs.walk(directory)
    
    # a global variable
    global producer 

    producer = KafkaProducer(bootstrap_servers=["%s:6667" %(HOST)], compression_type='gzip', acks=1, retries=2)
    
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

        with hdfs.open(myfile["name"]) as handle:
            for i, line in enumerate(handle):
                #strip line
                line = line.strip()
                
                submit(topic, line, trials=3)
                
                if i % 20000 == 0 and i != 0:
                    logger.info("%s lines submitted for %s" %(i, myfile["name"]))
                    
            #for every line
            
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
    args = parser.parse_args()
        
    #call main function
    main(directory=args.directory, topic=args.topic)

