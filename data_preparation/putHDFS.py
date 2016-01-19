# -*- coding: utf-8 -*-
"""
Created on Sun Jan 17 14:45:30 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

A program to explore data in aviation directory and put file into HDFS. 2 are the
database I want to explore

Airline Origin and Destination Survey (DB1B):

The Airline Origin and Destination Survey (DB1B) is a 10% sample of airline tickets
from reporting carriers collected by the Office of Airline Information of the Bureau
of Transportation Statistics. Data includes origin, destination and other itinerary
details of passengers transported. This database is used to determine air traffic patterns,
air carrier market shares and passenger flows.

Airline On-Time Performance Data

This table contains on-time arrival data for non-stop domestic flights by major
air carriers, and provides such additional items as departure and arrival delays,
origin and destination airports, flight numbers, scheduled and actual departure
and arrival times, cancelled or diverted flights, taxi-out and taxi-in times,
air time, and non-stop distance.

    Note: Over time both the code and the name of a carrier may change and the same
    code or name may be assumed by a different airline. To ensure that you are analyzing
    data from the same airline, TranStats provides four airline-specific variables
    that identify one and only one carrier or its entity: Airline ID (AirlineID),
    Unique Carrier Code (UniqueCarrier), Unique Carrier Name (UniqueCarrierName),
    and Unique Entity (UniqCarrierEntity). A unique airline (carrier) is defined
    as one holding and reporting under the same DOT certificate regardless of its Code,
    Name, or holding company/corporation.

More info at: http://www.transtats.bts.gov/databases.asp?pn=1&Mode_ID=1&Mode_Desc=Aviation&Subject_ID2=0

"""

import os
import sys
import shlex
import helper
import shutil
import tempfile
import zipfile
import logging

#An useful way to defined a logger lever, handler, and formatter
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s', level=logging.DEBUG)
program_name = os.path.basename(sys.argv[0])
logger = logging.getLogger(program_name)

#This is the origin-destination path (on AWS)
#database_path = "~/capstone/aviation/airline_origin_destination"

# This is the ontime database (AWS)
database_path = "/mnt/data/aviation/airline_ontime"

# The output path (in AWS sandbox)
#raw_data_path = "/mnt/data/raw_data/airline_origin_destination/"
raw_data_path = "/mnt/data/raw_data/airline_ontime/"

# truncate data file atfter this number of lines (debug)
MAX_LINES = 1000

#a function to process a directory
def processDirectory(directory):
    logger.debug("Processing %s directory..." %(directory))

    #get the file list
    all_files = os.listdir(directory)

    #process each file
    for myfile in all_files:
        #get a full path info
        myfile = os.path.join(directory,myfile)

        #process directories in recursive way
        if os.path.isdir(myfile):
            #call subroutine on this directory
            processDirectory(myfile)

        elif os.path.isfile(myfile):
            #call a function on file
            processFile(myfile)

        else:
            raise Exception, "Unknown object: %s"

# a function to process a single file
def processFile(myfile):
    logger.debug("Processing %s file" %(myfile))

    #get file extension
    file_type = os.path.splitext(myfile)[-1]

    #dealing with zip archive
    if file_type == ".zip":
        #call a function to process zip
        processZipFile(myfile)

    else:
        raise Exception, "Unknown file type: %s" %(myfile)


#A function to process a zipfile
def processZipFile(myfile):
    logger.debug("reading %s file content" %(myfile))

    #the global temporary working directory
    global workdir

    try:
        archive = zipfile.ZipFile(myfile)

    except zipfile.BadZipfile, message:
        #exit this function on errors
        logger.error("Error on %s: %s" %(myfile, message))
        logger.warn("Ignoring %s" %(myfile))
        return

    archived_files = archive.namelist()

    for archived_file in archived_files:
        logger.debug("Dealing with %s" %(archived_file))

        #determine file type
        file_type = os.path.splitext(archived_file)[-1]

        #if file is a readme, ignore it
        if file_type == ".html":
            logger.debug("ignoring %s" %(archived_file))

        elif file_type == ".csv":
            logger.debug("extract %s to %s" %(archived_file, workdir))
            archive.extract(archived_file, workdir)

            #construct data file path
            data_file = os.path.join(workdir, archived_file)

            #debug: deal with the first x lines of a file
            truncateFile(data_file)

            #pack csv in gzip format
            cmd = "pigz --best %s" %(data_file)
            cmds = shlex.split(cmd)
            helper.launch(cmds)

            archived_file += ".gz"

            logger.info("%s ready for HDFS" %(archived_file))


#A function to truncate a file
def truncateFile(myfile, lines=MAX_LINES):
    logger.debug("Truncating %s after %s lines" %(myfile, lines))

    #the global temporary working directory
    global workdir

    #creating a temporary file
    new_file = tempfile.mktemp(dir=workdir)
    logger.debug("%s created" %(new_file))

    #opening src and dest file
    source = helper.File(myfile)
    destination = open(new_file, "w")

    for line in source.head(lines):
        destination.write(line)

    #closing files
    source.close()
    destination.close()

    logger.debug("%s lines written" %(lines))

    #now moving new_file name in old_file name
    shutil.move(new_file, myfile)

    logger.debug("%s resized to first %s lines" %(myfile, lines))


#main function
if __name__ == "__main__":
    logger.info("%s started" %(program_name))


    #verify is output directory exists
    if os.path.exists(raw_data_path) and os.path.isdir(raw_data_path):
        raise Exception, "Output directory exists"

    #create a temporary directory
    workdir = tempfile.mkdtemp()

    logger.debug("Temporary directory %s created" %(workdir))

    #process the main directory
    processDirectory(database_path)

    #now move temporary directory to out directory
    shutil.move(workdir, outdir)

    logger.info("Data file ready in %s directory" %(outdir))

    logger.info("%s finished" %(program_name))
