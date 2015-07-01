import sys
import os
import logging
import time
from threading import Timer
import nanomsg as nn
import socket

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException
from base_module import FileModule

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Vertex")

def create_manager( workdata, config ):
    return Vertex( workdata, config )

MODE_NORMAL = 1
MODE_RECOVERY = 2
MODE_HALT = 3

class Vertex(FileModule):
    def __init__(self, workdata, config):
        FileModule.__init__(self,workdata,config)
        self.surveyor = nn.Socket( nn.SURVEYOR )
        self.surveyor.bind( "tcp://0.0.0.0:8688" )

        # 10 seconds max
        self.surveyor.set_int_option( nn.SURVEYOR, nn.SURVEYOR_DEADLINE, 10 )
        self.superstep = 0
        self.mode = MODE_NORMAL
        self.first = True

    def create_job_data( self, filename, idx ):
        inputfile = os.path.join( self.relinputdir, filename )
        jobdata = { "inputfile": inputfile }
        jobdata["hostname"] = socket.gethostname()
        jobdata["jobid"] = self.jobid
        jobdata["appdir"] = self.appname
        jobdata["appconfig"] = self.relconfig_file
        jobdata["type"] = "vertex"
        jobdata["workid"] = "%05d"%( idx )
        return inputfile, jobdata

    def get_work_key( self, data ):
        return data["inputfile"]

    def module_tracks_progress( self ):
        return True

    def all_hands_on_deck( self ):
        return True

    def finish( self ):
        logger.info("Finished vertex job")
        self.surveyor.close()

    def check_progress( self, numtasks ):
        if self.first:
            self.first = False
            # First time, wait 1 second for at least one worker to connect
            # BUG in surveyor protocol nanomsg
            time.sleep(1.0)

        # wait 0.2 second for messages to finish propagating
        time.sleep(0.2)

        if self.mode == MODE_NORMAL:
            logger.info("Processing in normal mode")
            return self.process_normal_mode( numtasks )
        elif self.mode == MODE_RECOVERY:   
            logger.info("Recovery mode")
            return False
        elif self.mode == MODE_HALT:   
            logger.info("Halt mode")
            return self.process_halt_mode( numtasks )

    def process_normal_mode( self, numtasks ):
        self.surveyor.send( "P" )
        respondents = 0
        try:
            while( True ):
                msg = remap_utils.decode( self.surveyor.recv() )
                respondents = respondents + 1
                if respondents == numtasks:
                    break
        except nn.NanoMsgAPIError as nme:
            logger.error("No vertex nodes connected")
            print(nme)
            self.mode = MODE_RECOVERY

        self.surveyor.send( "%d"%(self.superstep) )
        halt = True
        respondents = 0
        try:
            while( True ):
                msg = remap_utils.decode( self.surveyor.recv() )
                if msg != "HALT":
                    halt = False
                respondents = respondents + 1
                if respondents == numtasks:
                    # all replied
                    logger.info("All respondents replied")
                    break
        except nn.NanoMsgAPIError as nme:
            logger.error("No vertex nodes connected")
            print(nme)
            self.mode = MODE_RECOVERY

        if halt:
            self.mode = MODE_HALT
        else:
            self.superstep = self.superstep + 1
        return True

    def process_halt_mode( self, numtasks ):
        self.surveyor.send( "HALT" )
        respondents = 0
        try:
            while( True ):
                msg = remap_utils.decode( self.surveyor.recv() )
                respondents = respondents + 1
                if respondents == numtasks:
                    break
        except nn.NanoMsgAPIError as nme:
            print(nme)

        return False

