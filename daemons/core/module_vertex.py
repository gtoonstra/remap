import os
import sys
import logging
import time
import nanomsg as nn

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException
from lib.bonjour_detect import BonjourResolver
from base_module import WorkerBase

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Vertex")

def create_worker( app, appconfig, workdata ):
    return Vertex( app, appconfig, workdata )

MODE_IDLE = 0
MODE_MSGS = 1
MODE_HALT = 2
MODE_RUN  = 3
MODE_PROCESS = 4

class Vertex(WorkerBase):
    def __init__( self, app, appconfig, workdata ):
        WorkerBase.__init__( self, app, appconfig, workdata )
        self.surveyorname = workdata["hostname"]
        self.vsub = nn.Socket( nn.SUB, domain=nn.AF_SP )
        self.vpub = nn.Socket( nn.PUB, domain=nn.AF_SP )
        self.broker_address = None

        self.bonjour = BonjourResolver( "_vertexremap._tcp", self.cb_broker_changed )
        self.bonjour.start()

        inputfile = os.path.join( self.remaproot, "data", self.workdata["inputfile"] )
        outputdir = os.path.join( self.remaproot, "job", self.jobid, "part" )

        self.input = self.app.create_vertex_reader( inputfile )
        self.outputdir = outputdir
        self.partitions = {}

        self.mode = MODE_IDLE

        self.surveyor = nn.Socket( nn.RESPONDENT )
        self.surveyor.connect( "tcp://%s:8688"%(self.surveyorname) )
        # 6 seconds
        self.surveyor.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, 50 )
        self.vertices = {}

        logger.info("Waiting to get vertex broker host from bonjour")

        self.ready = False

    def cb_broker_changed( self, broker_address ):
        logger.info("Received vertex broker address: %s"%(broker_address) )
        if self.broker_address != None:
            return

        self.broker_address = broker_address

        # vertex broker pub and sub
        self.vpubc = self.vpub.connect( "tcp://%s:8689"%(self.broker_address) )
        self.vsubc = self.vsub.connect( "tcp://%s:8690"%(self.broker_address) )

        # 2 seconds max
        self.vsub.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, 2000 )

        logger.info("Vertex broker setup complete")

        for value in self.input.read():
            key, vertex = self.app.prepare( value )
            if key == None or vertex == None:
                continue
            # Store vertex by id in dict with 2 lists for messages
            self.vertices[ key ] = (vertex, [], [])
            self.vsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, key )

        logger.info("Ready for processing")

        self.ready = True

    def module_manages_progress( self ):
        return True

    def result( self ):
        return "complete", {"inputfile":self.workdata["inputfile"]}

    def forward( self, id, msg ):
        # Forward to vertex broker
        self.vpub.send( id + " " + msg )

    def subscribe( self, topic ):
        self.vsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, topic)

    def unsubscribe( self, topic ):
        self.vsub.set_string_option( nn.SUB, nn.SUB_UNSUBSCRIBE, topic)

    # This function performs actual work. The *state* is in the initiator daemon only,
    # so a worker is directly responsive to whatever the surveyor tells the worker to do.
    def work( self ):
        if not self.ready:
            return True

        surveyormsg = None
        try:
            surveyormsg = remap_utils.decode( self.surveyor.recv() )
        except nn.NanoMsgAPIError as e:
            return True

        if surveyormsg[0] == 'S':
            # Shift messages
            if self.mode != MODE_MSGS:
                self.mode = MODE_MSGS
                # We haven't done this in a previous step. Due to recovery, it might be 
                # used by the initiator to get others up to speed.
                for key, (vertex,messages,messagesNext) in self.vertices.items():
                    self.vertices[ key ] = ( vertex, messagesNext, [] )
            self.surveyor.send( "D" )
            return True

        if surveyormsg[0] == 'H':
            self.mode = MODE_HALT
            logger.info("Halting core.")
            self.surveyor.close()
            return False

        if surveyormsg[0] == 'P':
            if self.mode != MODE_PROCESS:
                # First time in this state, we need to grab all messages and
                # allocate them to vertex queue
                self.mode = MODE_PROCESS
                logger.info("Processing messages 1")
                while True:
                    try:
                        msg = self.vsub.recv()
                        prefix, data = remap_utils.unpack_msg( msg )
                        if prefix in self.vertices:
                            # This vertex is indeed on this host. Add the message to its new msg list for next iteration
                            vertex, messages, messagesNext = self.vertices[ prefix ]
                            messagesNext.append( data )
                    except nn.NanoMsgAPIError as e:
                        logger.error( "No more messages available." )
                        break
            else:
                logger.info("Processing messages 2")
                # doing things twice does not make a difference. Second time around, just throw away all messages
                while True:
                    try:
                        msg = self.vsub.recv()
                        print("Received and thrown away: ", msg)
                    except nn.NanoMsgAPIError as e:
                        logger.error( "No more messages available." )
                        break
            self.surveyor.send( "D" )
            return True

        self.mode = MODE_RUN
        self.superstep = int(surveyormsg)
        mainHalt = True

        for key, (vertex,messages,messagesNext) in self.vertices.items():
            vertex, halt = self.app.compute( self.forward, self.subscribe, self.unsubscribe, self.superstep, vertex, messages )
    
            if vertex != None:
                # Store the new vertex object in its place, maintaining the messagesNext list as we know it
                self.vertices[ key ] = (vertex,[],messagesNext)
            if not halt:
                mainHalt = False

        if mainHalt:
            self.surveyor.send( "H" )
        else:
            self.surveyor.send( "D" )
        return True

