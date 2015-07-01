import sys
import os
import logging
import time
from threading import Timer

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException
from base_module import DirModule

logging.basicConfig( level=logging.INFO ) 

logger = logging.getLogger("Reducer")

def create_manager( workdata, config ):
    return Reducer( workdata, config )

class Reducer(DirModule):
    def __init__( self, workdata, config ):
        DirModule.__init__(self, workdata, config )

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

