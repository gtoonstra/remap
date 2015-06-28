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
logger = logging.getLogger("Vertex")

def create_worker( app, appconfig, workdata ):
    return Vertex( app, appconfig, workdata )

class Vertex(WorkerBase):
    def __init__( self, app, appconfig, workdata ):
        WorkerBase.__init__( self, app, appconfig, workdata )
        self.workerid = workdata["workid"]

        inputfile = os.path.join( self.remaproot, "data", self.workdata["inputfile"] )
        outputdir = os.path.join( self.remaproot, "job", self.jobid, "part" )

        self.input = self.app.create_mapper_reader( inputfile )
        self.outputdir = outputdir
        self.partitions = {}

    def module_manages_progress( self ):
        return True

    def result( self ):
        return "complete", {"inputfile":self.workdata["inputfile"]}

    def work( self ):
        return False

