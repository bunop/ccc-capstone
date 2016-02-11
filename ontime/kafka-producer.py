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
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

from datetime import datetime

# Pydoop
import pydoop.hdfs as hdfs

#get file list
test_dataset = hdfs.ls("/user/paolo/capstone/airline_ontime/test")

#An useful way to defined a logger lever, handler, and formatter
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger(os.path.basename(sys.argv[0]))

def print_response(response=None):
    if response:
        print('Error: {0}'.format(response[0].error))
        print('Offset: {0}'.format(response[0].offset))

def main():
    kafka = KafkaClient("sandbox.hortonworks.com:6667")
    producer = SimpleProducer(kafka)
    
    try:
        time.sleep(5)
        topic = 'test'
        
        for myfile in test_dataset:
            with hdfs.open(myfile) as handle:
                for line in handle:
                    print_response(producer.send_messages(topic, line))
            
    except LeaderNotAvailableError:
        # https://github.com/mumrah/kafka-python/issues/249
        time.sleep(1)
        print_response(producer.send_messages(topic, line))
        
    kafka.close()

if __name__ == "__main__":
    main()

