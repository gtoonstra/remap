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

    # Create a bi-directional communication channel, where the node daemon 
    # 'shouts' in the room even to contact a single core, but the core only
    # sends written messages back to the shouter with the megaphone.
    # (embarassing protocol).
    def setup_bus( self ):
        self.bus = nn.Socket( nn.BUS )
        self.bus.bind("ipc:///tmp/node_daemon.ipc")

        # Apply a timeout for the loop.
        rcv_timeout = 100
        self.bus.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, rcv_timeout )

    def process_messages( self ):
        try:
            msg = self.bus.recv()
            self.process( msg )
        except nn.NanoMsgAPIError as e:
            pass

    def process( self, msg ):
        # It's always msgtype plus bunch of data, separated by space. 
        msgtype, data = remap_utils.unpack_msg( msg )
        if msgtype == "hello":
            return self.process_hello( data )

    def process_hello( self, data ):
        msgid = data[ "msgid" ]
        coreid = "12345"
        self.cores[ coreid ] = {"coreid":coreid,"ts_last_seen":time.time()}
        msg = remap_utils.pack_msg( "hey", {"result":"OK","msgid":msgid,"coreid":coreid,"broker_address":self.broker_address} )
        self.bus.send( msg )
        return

if __name__ == "__main__":

    node = NodeDaemon()
    node.setup_bus()

    while( True ):
        node.process_messages()
        # Every now and then check core heartbeats and remove cores no longer active.

