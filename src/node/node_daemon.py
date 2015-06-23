import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import logging
import time
from bonjour_detect import BonjourResolver
import sys

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils

# A node daemon connects to the broker and exists to allow
# cores to work independently. The idea is that the node daemon is a central
# contact point for concerns like machine health, hardware, proximity, 
# planned up/down time and that node routes messages from the bus to each core,
# which should reduce some potential waste if each core process has its own 
# code to discard messages, etc.
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
        self.broker_address = "unknown"
        self.brokerChanged = False
        self.sub = None
        self.pub = None
        self.nodeid = remap_utils.node_id()
        self.bonjour = BonjourResolver( "_remap._tcp", self.cb_broker_changed )
        self.bonjour.start()

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

    def cb_broker_changed( self, broker_address ):
        logger.info("Received new broker address: %s"%(broker_address) )
        self.broker_address = broker_address
        self.brokerChanged = True

    def setup_broker( self ):
        self.brokerChanged = False
        if self.sub != None:
            self.sub.close()
            self.sub = None

        self.apply_timeouts()

        if self.broker_address == "unknown":
            logger.error("Deferring broker setup as address is still unknown.")
            return

        self.sub = nn.Socket( nn.SUB )
        self.sub.connect( "tcp://%s:8687"%( self.broker_address ))
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "global")
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "local")
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "notlocal")
        # self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.coreid)
        self.apply_timeouts()

        self.pub = nn.Socket( nn.PUB )
        self.pub.connect( "tcp://%s:8686"%( self.broker_address ))

        logger.info("Broker setup complete")

    def process_bus_messages( self ):
        try:
            msg = self.bus.recv()
            msgtype, data = remap_utils.unpack_msg( msg )
            if msgtype.startswith("_"):
                # node message
                self.process_core_message( msgtype, data )
            else:
                # forward to broker instead
                self.forward_to_broker( msg )             
        except nn.NanoMsgAPIError as e:
            return False
        return True

    def process_core_message( self, msgtype, data ):
        if msgtype == "_hello":
            self.process_hello( data )

    def forward_to_broker( self, msg ):
        if self.pub != None:
            try:
                self.pub.send( msg )
            except nn.NanoMsgAPIError as e:
                pass

    # This processes a message where a core is announcing itself and wants to 
    # get a core id to start existing on the network    
    def process_hello( self, data ):
        msgid = remap_utils.safe_get(data, "msgid")
        pid = remap_utils.safe_get(data, "pid")
        coreid = remap_utils.core_id( self.nodeid, pid )
        self.cores[ coreid ] = {"coreid":coreid,"ts_last_seen":time.time()}
        msg = remap_utils.pack_msg( "_hey", {"result":"OK","msgid":msgid,"coreid":coreid} )
        self.bus.send( msg )
        return True

    def process_broker_messages( self ):
        if self.sub == None:
            # No broker is known yet.
            if self.brokerChanged:
                logger.info("The broker configuration changed.") 
                self.setup_broker()
                if self.sub == None:
                    logger.info("Failed broker setup.")
                    return False
            else:              
                return False

        try:
            msg = self.sub.recv()
            if msg != None and len(msg)>0:
                msgprefix, data = remap_utils.unpack_msg( msg )

                logger.info( "Just received %s:%s", msgprefix, data )
                self.bus.send(msg)

                return True
        except nn.NanoMsgAPIError as e:
            return False
        except ValueError as ve:
            logger.warn("Received invalid message: %s"%( msg ))
            return False
        return False

if __name__ == "__main__":
    logger.info("Starting node daemon")
    node = NodeDaemon()
    node.setup_bus()
    node.apply_timeouts()

    logger.info("Node daemon started")

    while( True ):
        while (node.process_bus_messages()):
            pass
        while (node.process_broker_messages()):
            pass
        if node.brokerChanged:
            node.setup_broker()

        # Every now and then check core heartbeats and remove cores no longer active.

