import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import logging

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils

# A core daemon connects to the node daemon.
# Core daemons manage the map/reduce processes doing the actual work.
# The node daemon reroutes broker messages on behalf of the core.
# 
# The core alternates between work and reading messages from the node
# The volume of messages isn't very high, it's mostly about planning and
# orchestration of the services and relaying progress, status and health messages.
# 

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("CoreDaemon")

class CoreDaemon( object ):
    def __init__(self):
        self.coreid = "NONE" 
        self.node = None

    # The core daemon connects to the node first.
    def setup_node( self ):
        self.node = nn.Socket( nn.BUS )
        self.node.connect("ipc:///tmp/node_daemon.ipc")

    def set_node_timeout( self, rcv_timeout ):
        # Apply a timeout for receiving messages from node.
        self.node.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, rcv_timeout )

    def process_node_messages( self ):
        try:
            msg = self.node.recv()
            logger.info( "Received message from node: %s"%( msg ))
            msgtype, data = remap_utils.unpack_msg( msg )
            if msgtype == self.coreid:
                # This is directed at this core specifically, so it's more of a req/rep type
                return self.process_request( data )
            return False
        except nn.NanoMsgAPIError as e:
            return False

    def register( self ):
        # Let's start with getting some meaningful identification stuff from node.
        self.set_node_timeout( 1000 )
        msgid = remap_utils.unique_id()

        logger.info( "Registering with node" )
        self.node.send( remap_utils.pack_msg( "hello", {"msgid":msgid} ) )

        # The while loop will terminate as soon as node stops sending messages,
        # so this should be safe to do.
        while True:
            try:
                msg = self.node.recv()
                msgtype, data = remap_utils.unpack_msg( msg )
                if msgtype != "hey":
                    continue
                if "msgid" in data:
                    if data["msgid"] == msgid:
                        self.coreid = data["coreid"]
                        self.broker_address = data["broker_address" ]
                        logger.info( "Got coreid %s and broker address %s."%( self.coreid, self.broker_address ))
                        return True
            except nn.NanoMsgAPIError as e:
                logger.error( "Node is currently not available." )
                break
        logger.error( "Registration failed" )
        return False

    def do_more_work( self ):
        pass

if __name__ == "__main__":

    # Initialization of the core. We need a core id to work with (from node),
    # and the address of the broker
    core = CoreDaemon()
    core.setup_node()
    
    # 5 attempts to register
    attempts = 5
    registered = False
    while ( attempts > 0 ):
        if core.register():
            registered = True
            break
        attempts = attempts - 1

    if not registered:
        logger.error( "Could not register with node to get a core id and connect to broker." )
        sys.exit(-1)

    core.set_node_timeout( 0 )

    while( True ):
        while (core.process_node_messages()):
            pas
        core.do_more_work()

