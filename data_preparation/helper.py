# -*- coding: utf-8 -*-
"""
Created on Sun Jan 17 15:34:41 2016

@author: Paolo Cozzi <paolo.cozzi@ptp.it>

helper libraries

"""

import os
import select
import signal
import logging
import subprocess

#logger instance
logger = logging.getLogger(__name__)

# A preexec function to ensure that subprocess are terminated 
# https://blog.nelhage.com/2010/02/a-very-subtle-bug/
preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# this environment is a copy of user envoironment
user_environ = os.environ.copy()

#found here: http://stackoverflow.com/questions/5896079/python-head-tail-and-backward-read-by-lines-of-a-text-file

class File(file):
    """ An helper class for file reading  """

    def __init__(self, *args, **kwargs):
        super(File, self).__init__(*args, **kwargs)
        self.BLOCKSIZE = 4096

    def head(self, lines_2find=1):
        self.seek(0)                            #Rewind file
        return [super(File, self).next() for x in xrange(lines_2find)]

    def tail(self, lines_2find=1):  
        self.seek(0, 2)                         #Go to end of file
        bytes_in_file = self.tell()
        lines_found, total_bytes_scanned = 0, 0
        while (lines_2find + 1 > lines_found and
               bytes_in_file > total_bytes_scanned): 
            byte_block = min(
                self.BLOCKSIZE,
                bytes_in_file - total_bytes_scanned)
            self.seek( -(byte_block + total_bytes_scanned), 2)
            total_bytes_scanned += byte_block
            lines_found += self.read(self.BLOCKSIZE).count('\n')
        self.seek(-total_bytes_scanned, 2)
        line_list = list(self.readlines())
        return line_list[-lines_2find:]

    def backward(self):
        self.seek(0, 2)                         #Go to end of file
        blocksize = self.BLOCKSIZE
        last_row = ''
        while self.tell() != 0:
            try:
                self.seek(-blocksize, 1)
            except IOError:
                blocksize = self.tell()
                self.seek(-blocksize, 1)
            block = self.read(blocksize)
            self.seek(-blocksize, 1)
            rows = block.split('\n')
            rows[-1] = rows[-1] + last_row
            while rows:
                last_row = rows.pop(-1)
                if rows and last_row:
                    yield last_row
        yield last_row
        
#Invoke subprocess here: launch a simple command
def launch(cmds):
    """Launch a simple command as a list using subprocess. Stdout and Stderr are 
    directed using subprocess.PIPE"""
    
    logger.debug("Launching: %s" %( " ".join(cmds)))
    
    proc = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=preexec_fn, env=user_environ)
    
    #the following code is inspired from here:
    #http://stackoverflow.com/questions/12270645/can-you-make-a-python-subprocess-output-stdout-and-stderr-as-usual-but-also-cap
    
    while True:
        #in the reads array I will have file descriptor for subprocess stdout and stderr
        reads = [proc.stdout.fileno(), proc.stderr.fileno()]
        
        #select supports asynchronous I/O on multiple file descriptors
        ret = select.select(reads, [], [])
    
        for fd in ret[0]:
            if fd == proc.stdout.fileno():
                read = proc.stdout.readline()
                if len(read) > 0:
                    logger.debug("%s: %s" %(cmds[0], read.strip()))
                
            if fd == proc.stderr.fileno():
                read = proc.stderr.readline()
                if len(read) > 0:
                    logger.warn("%s: %s" %(cmds[0], read.strip()))
                
        #check if child process is terminated
        status = proc.poll()
    
        if status != None:
            #empty the stdout and stderr
            for read in proc.stdout:
                logger.debug(read.strip())
                    
            for read in proc.stderr:
                logger.warn(read.strip())
        
            #break the while True cycle
            break
    
    logger.debug("%s terminated with status %s" %(cmds[0], status))
    
    #Check status code
    if status != 0:
        raise Exception("%s exited with status code %s" %(cmds[0], status))
            
            