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

    def all_hands_on_deck( self ):
        return False

    def finish( self ):
        pass

class FileModule(BaseModule):
    def __init__( self, workdata, config ):
        BaseModule.__init__(self, workdata, config )

        self.type = workdata["type"]
        self.check_param( "inputdir" )
        inputdir = workdata["inputdir"]
        self.inputdir = os.path.join( self.datadir, inputdir.strip("/") )

        if not os.path.isdir( self.inputdir ):
            raise RemapException("Input dir does not exist: %s"%(self.inputdir))

        self.relinputdir = inputdir

    def prepare( self ):
        self.base_prepare( True )

        self.partitions_dir = os.path.join( self.job_dir, "part" )
        # In the normal mapper process, the output should not yet exist.

        os.makedirs( self.partitions_dir )

    def plan_jobs( self, planner ):
        return planner.task_per_file_in_dir( self.create_job_data, self.inputdir )

class DirModule(BaseModule):
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
        self.base_prepare( False )

        os.makedirs( self.outputdir )

    def plan_jobs( self, planner ):
        return planner.task_per_dir( self.create_job_data, os.path.join( self.remaproot, "job", self.jobid, "part" ) )

    def module_tracks_progress( self ):
        return False

