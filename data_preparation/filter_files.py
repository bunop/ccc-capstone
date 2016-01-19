# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 21:48:07 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

A program for filter out aviation dataset

"""

import os

#read imput files
filein = open("files.txt", "ru")
fileout = open("filtered_files.txt", "w")

#remenber last directory and last file
last_dir = None
last_file = None

#now process data. If a record has an extension, consider as a file, otherwise a directory
for line in filein:
    line = line.strip()
    
    if os.path.splitext(line)[1] != "":
        #its a file
        #print "%s is a file" %(line)
        if last_file is None:
            last_file = line
            print "Taking %s file" %(last_file)
            fileout.write("%s\n" %(last_file))
        
    else:
        #its a directory
        #print "%s is a directory" %(line)
        last_dir = line
        last_file = None
    