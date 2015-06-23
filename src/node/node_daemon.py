import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import logging
import time

import sys

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils

# A node daemon doesn't connect to the broker, but exists to allow
# cores to work independently. The idea is that the node daemon is a central
# contact point for concerns like machine health, hardware, proximity, 
# planned up/down time? and get notified about other things that are going on
# on the hardware without each core having to install the same callbacks or hooks.
# 
# The node daemon, together with other processes, is expected to be allocated one core
# of the machine, leaving (num_cores-1) free for core processes.
# 

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("NodeDaemon")

class NodeDaemon( object ):
    def __init__(self):
        self.cores = {}
        self.broker_address = "localhost"
        self.sub = None

    # Create a bi-directional communication channel, where the node daemon 
    # 'shouts' in the room even to contact a single core, but the core only
    # sends written messages back to the shouter with the megaphone.
    # (embarassing protocol).
    def setup_bus( self ):
        self.bus = nn.Socket( nn.BUS )
        self.bus.bind("ipc:///tmp/node_daemon.ipc")

    def apply_timeouts( self ):
        if self.sub == None:
            rcv_timeout = 100
            self.bus.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, rcv_timeout )     
        else:
            rcv_timeout = 100
            self.sub.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, rcv_timeout )
            rcv_timeout = 0
            self.bus.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, rcv_timeout )     

    def setup_broker( self ):
        if self.sub != None:
            self.sub.close()
            self.sub = None

        self.apply_timeouts()

        if self.broker_address == "unknown":
            logger.error("Cannot setup broker yet. Address unknown.")
            return

        self.sub = nn.Socket( nn.SUB )
        self.sub.connect( "tcp://%s:8687"%( self.broker_address ))
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "global")
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "local")
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "notlocal")
        # self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.coreid)
        self.apply_timeouts()
        logger.info("Broker setup complete")

    def process_node_messages( self ):
        try:
            msg = self.bus.recv()
            msgtype, data = remap_utils.unpack_msg( msg )
            if msgtype == "hello":
                return self.process_hello( data )
        except nn.NanoMsgAPIError as e:
            pass

    def process_hello( self, data ):
        msgid = data[ "msgid" ]
        coreid = "12345"
        self.cores[ coreid ] = {"coreid":coreid,"ts_last_seen":time.time()}
        msg = remap_utils.pack_msg( "hey", {"result":"OK","msgid":msgid,"coreid":coreid,"broker_address":self.broker_address} )
        self.bus.send( msg )
        return

    def process_broker_messages( self ):
        if self.sub == None:
            # No broker is known yet.
            return False

        try:
            msg = self.sub.recv()
            if msg != None and len(msg)>0:
                msgprefix, data = remap_utils.unpack_msg( msg )

                logger.info( "Just received %s:%s", msgprefix, data )

                if msgprefix.startswith(self.coreid):
                    # message unidirectionally sent to me.
                    self.process_personal_message( msgprefix, data )
                elif msgprefix.startswith( "global" ):
                    self.process_global_message( msgprefix, data )
                elif msgprefix.startswith( "local" ):
                    self.process_local_message( msgprefix, data )    
                elif msgprefix.startswith( "notlocal" ):
                    self.process_global_message( msgprefix, data ) 
                return True
        except nn.NanoMsgAPIError as e:
            return False
        except ValueError as ve:
            logger.warn("Received invalid message: %s"%( msg ))
            return False
 
    def process_personal_message( self, prefix, data ):
        pass

    def process_global_message( self, prefix, data ):
        pass

    def process_local_message( self, prefix, data ):
        pass


if __name__ == "__main__":

    node = NodeDaemon()
    node.setup_bus()
    node.setup_broker()

    while( True ):
        while (node.process_node_messages()):
            pass
        while (node.process_broker_messages()):
            pass
        # Every now and then check core heartbeats and remove cores no longer active.

