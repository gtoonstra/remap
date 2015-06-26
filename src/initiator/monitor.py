import os
import sys
import logging
import time

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Initiator")

class Monitor(object):
    def __init__(self, rootdir):
        self.rootdir = os.path.abspath( rootdir )
        self.appdir = os.path.join( self.rootdir, "app" )
        self.jobdir = os.path.join( self.rootdir, "job" )
        self.nodes = {}
        self.cores = {}
        self.inputfiles = {}

    def list_apps( self ):
        apps = []
        for root, dirs, files in os.walk( self.appdir ):
            for f in files:
                if f == "appconfig.json":
                    apps.append( os.path.relpath(root, self.appdir) )
        return apps

    def list_jobs( self ):
        jobs = []
        for root in os.listdir( self.jobdir ):
            jobs.append( root )
        return jobs

    def list_nodes( self ):
        return self.nodes

    def list_cores( self ):
        return self.cores


