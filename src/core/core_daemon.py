import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import logging
import time

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils

# A core daemon connects to the node daemon.
# Core daemons manage the map/reduce processes doing the actual work.
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
        self.pid = os.getpid()
        self.coreid = "unknown" 
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
            msgprefix, data = remap_utils.unpack_msg( msg )
            recipientid,msgtype,senderid = msgprefix.split(".")

            if recipientid == self.coreid:
                # This is directed at this core specifically, so it's more of a req/rep type
                self.process_personal_message( msgtype, senderid, data )
            if recipientid == "global":
                self.process_global_message( msgtype, senderid, data )
            elif recipientid == "local":
                self.process_local_message( msgtype, senderid, data )    
            elif recipientid == "notlocal":
                self.process_global_message( msgtype, senderid, data )
            else:
                logger.info("Unrecognized message type %s, sent by %s"%( msgtype, senderid ) )
            return True
        except nn.NanoMsgAPIError as e:
            return False

    # this function registers the new core process with node
    def register( self ):
        self.set_node_timeout( 500 )
        msgid = remap_utils.unique_id()

        logger.info( "Registering with node" )
        self.node.send( remap_utils.pack_msg( "_hello", {"msgid":msgid,"pid":self.pid} ) )

        # The while loop will terminate as soon as node stops sending messages,
        # so this should be safe to do.
        while True:
            try:
                msg = self.node.recv()
                msgtype, data = remap_utils.unpack_msg( msg )
                if msgtype != "_hey":
                    continue
                if "msgid" in data:
                    # make sure that this 'hey' message is for this core.
                    # multiple cores may be registering at the same time.
                    if data["msgid"] == msgid:
                        self.coreid = data["coreid"]
                        logger.info( "Got coreid %s."%( self.coreid ))
                        return True
            except nn.NanoMsgAPIError as e:
                logger.error( "Node is currently not available." )
                break
        logger.error( "Registration failed" )
        return False

    def process_personal_message( self, msgtype, sender, data ):
        pass

    def process_global_message( self, msgtype, sender, data ):
        pass

    def process_local_message( self, msgtype, sender, data ):
        pass

    def do_more_work( self ):
        pass

if __name__ == "__main__":

    # Initialization of the core. We need a core id to work with (from node).
    core = CoreDaemon()
    core.setup_node()

    # wait 50ms for node comms to be established
    time.sleep( 0.05 )

    # 5 attempts to register
    attempts = 5
    registered = False
    while ( attempts > 0 ):
        if core.register():
            registered = True
            break
        attempts = attempts - 1

    if not registered:
        logger.error( "Could not register with node to get a core id. Exiting." )
        sys.exit(-1)

    core.set_node_timeout( 0 )

    while( True ):
        while (core.process_node_messages()):
            pass
        core.do_more_work()

