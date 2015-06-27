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

logger = logging.getLogger("Reducer")

def create_manager( workdata, config ):
    return Reducer( workdata, config )

class Reducer(BaseModule):
    def __init__( self, workdata, config ):
        BaseModule.__init__(self, workdata, config )

        self.check_param( "outputdir" )
        outputdir = workdata["outputdir"]

        verifyDir = os.path.join( self.remaproot, "job", self.jobid )
        self.outputdir = os.path.join( self.datadir, outputdir.strip("/") )

        if not os.path.isdir( verifyDir ):
            raise RemapException("Input dir does not exist: %s"%(verifyDir))
        if os.path.isdir( self.outputdir ):
            raise RemapException("Output dir already exists: %s"%(self.outputdir))

        self.reloutputdir = outputdir

    def prepare( self ):
        logger.info("Starting a mapper job: %s"%( self.appname ))
        self.base_prepare( False )

        os.makedirs( self.outputdir )

    def plan_jobs( self, planner ):
        return planner.task_per_dir( self.create_job_data, os.path.join( self.remaproot, "job", self.jobid, "part" ) )

    def create_job_data( self, dirname ):
        job = { "partition": dirname }
        job["jobid"] = self.jobid
        job["partition"] = dirname
        job["appdir"] = self.appname
        job["appconfig"] = self.relconfig_file
        job["type"] = "reducer"
        job["outputdir"] = self.reloutputdir
        return dirname, job

    def get_work_key( self, data ):
        return data["partition"]

