import sys
import os
import logging
import time
import shutil

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException

class BaseModule(object):
    def __init__( self, workdata, config ):
        self.workdata = workdata
        self.jobid = config["jobid"]
        self.remaproot = config["remaproot"]

        self.rootdir = os.path.abspath( self.remaproot )
        self.appsdir = os.path.join( self.rootdir, "app" )
        self.jobsdir = os.path.join( self.rootdir, "job" )
        self.datadir = os.path.join( self.rootdir, "data" )

        self.check_param( "app" )
        self.appname = workdata["app"]

        self.app_dir = os.path.join( self.appsdir, self.appname )
        self.job_dir = os.path.join( self.jobsdir, self.jobid )
        self.app_job_dir = os.path.join( self.job_dir, "app", self.appname )
        self.config_file = os.path.join( self.app_job_dir, "appconfig.json" )
        self.relconfig_file = os.path.join( self.appname, "appconfig.json" )

    def check_param( self, paramname ):
        if paramname not in self.workdata:
            raise RemapException("Job requires %s parameter"%( paramname ))

    def base_prepare( self, failIfJobsDirExists ):
        if os.path.isdir( self.job_dir ):
            if failIfJobsDirExists:
                raise RemapException("Jobs directory already exists" )
        else:
            os.makedirs( self.job_dir )

            # Copying the app files
            try:
                shutil.copytree(self.app_dir, self.app_job_dir)
                # Directories are the same
            except shutil.Error as e:
                raise RemapException("Directory not copied: %s"%(e))
            except OSError as e:
                raise RemapException("Directory not copied: %s"%(e))


