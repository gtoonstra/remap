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
from base_module import WorkerBase

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Vertex")

def create_worker( app, appconfig, workdata ):
    return Vertex( app, appconfig, workdata )

class Vertex(WorkerBase):
    def __init__( self, app, appconfig, workdata ):
        WorkerBase.__init__( self, app, appconfig, workdata )
        self.surveyorname = workdata["hostname"]

        inputfile = os.path.join( self.remaproot, "data", self.workdata["inputfile"] )
        outputdir = os.path.join( self.remaproot, "job", self.jobid, "part" )

        self.input = self.app.create_vertex_reader( inputfile )
        self.outputdir = outputdir
        self.partitions = {}

        self.surveyor = nn.Socket( nn.RESPONDENT )
        self.surveyor.connect( "tcp://%s:8688"%(self.surveyorname) )
        # 6 seconds
        self.surveyor.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, 50 )
        self.vertices = {}

    def add_message( self, prefix, data ):
        prefix = prefix[1:]
        if prefix in self.vertices:
            (vertex, messages, messagesNext) = self.vertices[ prefix ]
            messagesNext.append( data )

    def module_manages_progress( self ):
        return True

    def result( self ):
        return "complete", {"inputfile":self.workdata["inputfile"]}

    def prepare( self, sub_fn ):
        for value in self.input.read():
            key, vertex = self.app.prepare( value )
            if key == None or vertex == None:
                continue
            # Store vertex by id in dict with 2 lists for messages
            self.vertices[ key ] = (vertex, [], [])
            sub_fn( "v" + key, True )

    def vertex_forward( self, forward_fn ):
        def inner( prefix, data ):
            prefix = "v" + prefix
            forward_fn( prefix, data )
        return inner

    def work( self, forward_fn, sub_fn ):
        msg = None
        try:
            msg = remap_utils.decode( self.surveyor.recv() )
        except nn.NanoMsgAPIError as e:
            return True

        if msg[0] == 'P':
            for key, (vertex,messages,messagesNext) in self.vertices.items():
                self.vertices[ key ] = ( vertex, messagesNext, [] )
            self.surveyor.send( "OK" )
            return True

        if msg == "HALT":
            logger.info("Halting core.")
            self.surveyor.close()
            return False

        self.superstep = int(msg)
        mainHalt = True
        decorated = self.vertex_forward( forward_fn )
        for key, (vertex,messages,messagesNext) in self.vertices.items():
            vertex, halt = self.app.compute( decorated, self.superstep, vertex, messages )
            if vertex != None:
                self.vertices[ key ] = (vertex,[],messagesNext)
            if not halt:
                mainHalt = False

        if mainHalt:
            self.surveyor.send( "HALT" )
        else:
            self.surveyor.send( "OK" )
        return True
       

