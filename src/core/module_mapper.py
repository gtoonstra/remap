import os
import sys
import logging
import time

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException

def create_worker( app, appconfig, workdata, skeleton ):
    return Mapper( app, appconfig, workdata, skeleton )

class Mapper(object):
    def __init__( self, app, appconfig, workdata, skeleton ):
        self.app = app
        self.appconfig = appconfig
        self.workdata = workdata
        self.skeleton = skeleton
        self.jobid = workdata["jobid"]
        self.remaproot = appconfig["remaproot"]
        self.progress = 0
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

    def work( self ):
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

