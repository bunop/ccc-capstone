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

from kafka.common import LeaderNotAvailableError, UnknownError, KafkaTimeoutError
from kafka import KafkaProducer

from datetime import datetime

from common import *

# Pydoop
import pydoop.hdfs as hdfs

#get file list
test_dataset = hdfs.ls(DATA_DIR)

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

def submit(topic, data, trials=3, async=True):
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

def main():
    # a global variable
    global producer 

    # Get a producer object
    producer = KafkaProducer(bootstrap_servers=["node4:6667"], compression_type='gzip', acks=1, retries=2)
    
    for myfile in test_dataset:
        if "_SUCCESS" in myfile:
            continue
        
        logger.info("Working on %s" %(myfile))
        with hdfs.open(myfile) as handle:
            data = []
            for i, line in enumerate(handle):
                #strip line
                line = line.strip()
                
                data += [line]
                
                if i % 5000 == 0:                                 
                    #Submit data (my function)
                    submit(TOPIC, data, trials=3)
                    data = []
                
                if i % 20000 == 0:
                    logger.info("%s lines submitted for %s" %(i, myfile))
                    
            #for every line
            
            #submit the rest of the data
            submit(TOPIC, data, trials=3)
            data = []
            
        #with file open
        logger.info("Completed %s" %(myfile))
        
        #sleep some time
        time.sleep(5)
                    
    # for all files in HDFS
    producer.close()

if __name__ == "__main__":
    main()

