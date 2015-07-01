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

MODE_SHIFT = 1
MODE_COMPUTE = 2
MODE_PROCESS = 3
MODE_RECOVERY = 4
MODE_HALT = 5

class Vertex(FileModule):
    def __init__(self, workdata, config):
        FileModule.__init__(self,workdata,config)
        self.surveyor = nn.Socket( nn.SURVEYOR )
        self.surveyor.bind( "tcp://0.0.0.0:8688" )

        # 10 seconds max
        self.surveyor.set_int_option( nn.SURVEYOR, nn.SURVEYOR_DEADLINE, 10000 )
        self.superstep = 0
        self.mode = MODE_SHIFT
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
        self.surveyor = None

    def check_progress( self, numtasks ):
        if self.first:
            self.first = False
            # First time, wait 5 seconds for at least one worker to connect
            # BUG in surveyor protocol nanomsg
            time.sleep(5.0)
            # first time through, return, because num tasks would have been updated.
            # if not, it goes through to the other methods, zero tasks == no users, no response and exit.
            return True

        # wait 0.2 second for messages to finish propagating
        time.sleep(0.2)

        if self.mode == MODE_SHIFT:
            logger.info("Shifting messages")
            return self.process_shift_mode( numtasks )
        elif self.mode == MODE_COMPUTE:
            logger.info("Computing mode")
            return self.process_compute_mode( numtasks )
        elif self.mode == MODE_PROCESS:
            logger.info("Processing messages at each node")
            return self.process_process_mode( numtasks )
        elif self.mode == MODE_RECOVERY:   
            logger.info("Recovery mode")
            # TODO: implementation.
            return False
        elif self.mode == MODE_HALT:   
            logger.info("Halt mode")
            return self.process_halt_mode( numtasks )

    def process_shift_mode( self, numtasks ):
        self.surveyor.send( "S" )
        respondents = 0
        try:
            while( True ):
                msg = remap_utils.decode( self.surveyor.recv() )
                respondents = respondents + 1
                if respondents == numtasks:
                    logger.info("All respondents replied")
                    self.mode = MODE_COMPUTE
                    break
        except nn.NanoMsgAPIError as nme:
            logger.error("No vertex nodes replied")
            print(nme)
            self.mode = MODE_RECOVERY
        return True

    def process_compute_mode( self, numtasks ):
        # Send superstep
        self.surveyor.send( "%d"%(self.superstep) )
        halt = True
        respondents = 0
        try:
            while( True ):
                msg = remap_utils.decode( self.surveyor.recv() )
                if msg != "H":
                    halt = False
                respondents = respondents + 1
                if respondents == numtasks:
                    # all replied
                    logger.info("All respondents replied")
                    self.mode = MODE_PROCESS
                    break
        except nn.NanoMsgAPIError as nme:
            logger.error("No vertex nodes connected")
            print(nme)
            self.mode = MODE_RECOVERY
            return True

        if halt:
            self.mode = MODE_HALT
        else:
            self.superstep = self.superstep + 1
        return True

    def process_process_mode( self, numtasks ):
        self.surveyor.send( "P" )
        respondents = 0
        try:
            while( True ):
                msg = remap_utils.decode( self.surveyor.recv() )
                respondents = respondents + 1
                if respondents == numtasks:
                    logger.info("All respondents replied")
                    self.mode = MODE_SHIFT
                    break
        except nn.NanoMsgAPIError as nme:
            logger.error("No vertex nodes replied")
            print(nme)
            self.mode = MODE_RECOVERY
        return True

    def process_halt_mode( self, numtasks ):
        self.surveyor.send( "H" )
        respondents = 0
        try:
            while( True ):
                msg = remap_utils.decode( self.surveyor.recv() )
                respondents = respondents + 1
                if respondents == numtasks:
                    logger.info("All respondents replied")
                    break
        except nn.NanoMsgAPIError as nme:
            print(nme)
        return False

