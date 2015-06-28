import os
import sys
import logging
import time

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException
from base_module import WorkerBase

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Mapper")

def create_worker( app, appconfig, workdata ):
    return Mapper( app, appconfig, workdata )

class Mapper(WorkerBase):
    def __init__( self, app, appconfig, workdata ):
        WorkerBase.__init__( self, app, appconfig, workdata )
        self.workerid = workdata["workid"]

        inputfile = os.path.join( self.remaproot, "data", self.workdata["inputfile"] )
        outputdir = os.path.join( self.remaproot, "job", self.jobid, "part" )

        self.input = self.app.create_mapper_reader( inputfile )
        self.outputdir = outputdir
        self.partitions = {}

    def status( self ):
        return {"inputfile":self.workdata["inputfile"],"progress":self.progress}

    def result( self ):
        if self.input.isComplete():
            return "complete", {"inputfile":self.workdata["inputfile"]}

        return "fail", {"inputfile":self.workdata["inputfile"]}

    def work( self, forward_fn, sub_fn ):
        if self.input.isComplete():
            return False

        # so, do some work
        for k1, v1 in self.input.read():
            for part, k2, v2 in self.app.map( k1, v1 ):
                if part not in self.partitions:
                    self.partitions[ part ] = self.app.create_mapper_partitioner( self.outputdir, part, self.workerid )
                self.partitions[ part ].store( k2, v2 )

            p = self.input.progress()
            if p > self.progress+5:
                self.progress = int(p)
                break

        if self.input.isComplete():
            self.progress = 100
            self.input.close()
            for part in self.partitions:
                self.partitions[part].sort_flush_close()

        return True

