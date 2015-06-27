import sys
import os
import logging
import time
from threading import Timer
from planner import JobPlanner

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException
from base_module import BaseModule

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Mapper")

def create_manager( workdata, config ):
    return Mapper( workdata, config )

class Mapper(BaseModule):
    def __init__( self, workdata, config ):
        BaseModule.__init__(self, workdata, config )

        self.check_param( "inputdir" )
        inputdir = workdata["inputdir"]
        self.inputdir = os.path.join( self.datadir, inputdir.strip("/") )

        if not os.path.isdir( self.inputdir ):
            raise RemapException("Input dir does not exist: %s"%(self.inputdir))

        self.relinputdir = inputdir

    def prepare( self ):
        logger.info("Starting a mapper job: %s"%( self.appname ))
        self.base_prepare( True )

        self.partitions_dir = os.path.join( self.job_dir, "part" )
        # In the normal mapper process, the output should not yet exist.

        os.makedirs( self.partitions_dir )

    def plan_jobs( self, planner ):
        return planner.task_per_file_in_dir( self.create_job_data, self.inputdir )

    def create_job_data( self, filename, idx ):
        inputfile = os.path.join( self.relinputdir, filename )
        jobdata = { "inputfile": inputfile }
        jobdata["jobid"] = self.jobid
        jobdata["appdir"] = self.appname
        jobdata["appconfig"] = self.relconfig_file
        jobdata["type"] = "mapper"
        jobdata["workid"] = "%05d"%( idx )
        return inputfile, jobdata

    def get_work_key( self, data ):
        return data["inputfile"]

