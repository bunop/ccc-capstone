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

from kafka.common import LeaderNotAvailableError
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

def print_response(response=None):
    if response:
        msg = ['Error: {0}'.format(response[0].error)]
        msg += ['Offset: {0}'.format(response[0].offset)]
        msg = "; ".join(msg)
        logger.debug(msg)

def submit(topic, line, trials=3):
    step = 0
    global producer
    
    while step < trials:
        step += 1
        try:
            print_response(producer.send_messages(topic, line))
            return #if submitted
            
        except LeaderNotAvailableError:
            # https://github.com/mumrah/kafka-python/issues/249
            time.sleep(1)
            
    #If arrive here
    logger.warn("line %s ignored" %(line))
    return #anyway

def main():
    # a global variable
    global producer 

    producer = KafkaProducer(bootstrap_servers=["master:6667", "node1:6667", "node2:6667", "node3:6667"], compression_type='gzip', retries=2, acks=1)
    
    for myfile in test_dataset:
        if "_SUCCESS" in myfile:
            continue
        
        logger.info("Working on %s" %(myfile))
        with hdfs.open(myfile) as handle:
            for i, line in enumerate(handle):
                #strip line
                line = line.strip()
                
                #Submite data
                future = producer.send(TOPIC, line)
                
                # Block for 'synchronous' sends
                try:
                    record_metadata = future.get(timeout=10)
                    
                except Exception, message:
                    # Decide what to do if produce request failed...
                    logger.exception(message)
                    pass
                
                if i % 10000 == 0:
                    logger.info("%s lines submitted for %s" %(i, myfile))
                    
            #for every line
            
        #with file open
        logger.info("Completed %s" %(myfile))
        
        #sleep some time
        time.sleep(10)
                    
    # for a generic file in HDFS
    kafka.close()

if __name__ == "__main__":
    main()

