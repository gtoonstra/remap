import os
import sys
import logging
import time
import heapq
from operator import itemgetter

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException
from base_module import WorkerBase

def create_worker( app, appconfig, workdata ):
    return Reducer( app, appconfig, workdata )

class Reducer(WorkerBase):
    def __init__( self, app, appconfig, workdata ):
        WorkerBase.__init__( self, app, appconfig, workdata )
        self.total_size = 0
        self.prevkey = None

        # This is a reducer operation
        inputdir = os.path.join( self.remaproot, "job", self.jobid, "part", self.workdata["partition"] )
        outputdir = os.path.join( self.remaproot, "data", self.workdata["outputdir"] )

        self.reducerfiles = sorted(os.listdir( inputdir ))
        self.inputdir = inputdir
        self.numparts = len(self.reducerfiles)
        self.fraction = 100.0 / self.numparts
        self.completedparts = 0
        self.outputdir = outputdir
        self.partition = self.workdata["partition"]
        self.reducerWriter = self.app.create_reducer_writer( self.outputdir, self.partition )

        self.sources = []
        for filename in self.reducerfiles:
            f = self.app.create_reducer_reader( os.path.join( self.inputdir, filename ))
            self.sources.append( f )
            self.total_size = self.total_size + f.filesize

        decorated = [
            ((key,list_of_values,recsize) for key,list_of_values,recsize in f.read())
            for f in self.sources]
        self.merged = heapq.merge(*decorated)

    def status( self ):
        return {"partition":self.partition,"progress":self.progress}

    def result( self ):
        if len(self.sources) == 0:
            return "complete", {"partition":self.partition}

        return "fail", {"partition":self.partition}

    def work( self, forward_fn, sub_fn ):
        if len(self.sources) == 0:
            return False

        readrec = False
        for k2,v2,recsize in self.merged:
            readrec = True
            if self.prevkey == None:
                # Initialize the very first step
                self.prevkey = k2
                self.prevlist = v2
                self.processed = recsize
            elif self.prevkey != k2:
                # The key changed. Dump all values of previous step
                for k3,v3 in self.app.reduce( self.prevkey, self.prevlist ):
                    self.reducerWriter.store( k3, v3 )
                self.prevkey = k2
                self.prevlist = v2
                self.processed = self.processed + recsize
            else:
                # Add another record to the list
                self.prevlist = self.prevlist + v2
                self.processed = self.processed + recsize

            p = (self.processed / self.total_size) * 100
            if p > self.progress+5:
                self.progress = int(p)
                # breaking out of the loop to check up on messages
                break

        if not readrec:
            # done
            self.progress = 100
            for f in self.sources:
                f.close()
            self.sources = []
            self.reducerWriter.close()

        return True

