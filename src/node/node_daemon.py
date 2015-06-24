import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import logging
import time
from bonjour_detect import BonjourResolver
import sys
from node_hardware import NodeHardware

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException

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
        self.tot_m_rcv = 0
        self.hw = NodeHardware()
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
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.nodeid)
        self.apply_timeouts()

        self.pub = nn.Socket( nn.PUB )
        self.pub.connect( "tcp://%s:8686"%( self.broker_address ))

        logger.info("Broker setup complete")

    def process_bus_messages( self ):
        try:
            msg = self.bus.recv()
            msgprefix, data = remap_utils.unpack_msg( msg )
            recipientid,msgtype,senderid = remap_utils.split_prefix(msgprefix)

            if msgtype.startswith("_"):
                # node message
                self.process_core_message( msgtype, data )
            elif msgtype == "status":
                if senderid in self.cores:                
                    coredata = self.cores[ senderid ]
                    coredata["ts_last_seen"] = time.time()
                    coredata["progress"] = data["progress"]
                    logger.info("Core %s progressed %d"%( senderid, coredata["progress"] ))
                    self.forward_to_broker( msg )
            else:
                # forward to broker instead
                self.forward_to_broker( msg )             
            return True
        except nn.NanoMsgAPIError as e:
            return False

    def process_core_message( self, msgtype, data ):
        if msgtype == "_hello":
            self.process_hello( data )

    def forward_to_broker( self, msg ):
        if self.pub != None:
            try:
                logger.info("Sending %s to broker"%(msg))
                self.pub.send( msg )
            except nn.NanoMsgAPIError as e:
                pass

    # This processes a message where a core is announcing itself and wants to 
    # get a core id to start existing on the network    
    def process_hello( self, data ):
        msgid = remap_utils.safe_get(data, "msgid")
        pid = remap_utils.safe_get(data, "pid")
        priority = remap_utils.safe_get( data, "priority" )
        coreid = remap_utils.core_id( self.nodeid, pid )
        self.cores[ coreid ] = {"coreid":coreid,"ts_last_seen":time.time(),"progress":-1,"pid":pid,"priority":priority}
        msg = remap_utils.pack_msg( "%s._hey.%s"%(coreid, self.nodeid), {"result":"OK","msgid":msgid,"coreid":coreid} )
        if self.sub != None:
            self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, coreid)
        logger.info( "A core registered %s"%( coreid ))
        self.bus.send( msg )

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
            # Grab next msg from broker if any
            msg = self.sub.recv()
            self.tot_m_rcv = self.tot_m_rcv + 1
            if msg != None and len(msg)>0:
                msgprefix, data = remap_utils.unpack_msg( msg )
                logger.info("Received %s"%(msgprefix))
                recipientid,msgtype,senderid = remap_utils.split_prefix(msgprefix)
                if msgtype == "showhands":
                    self.handle_showhands( recipientid, senderid, data )
                elif msgtype == "jobstart":
                    #if recipientid == self.nodeid:
                    self.handle_jobstart( recipientid, senderid, data )
                else:
                    # Forward to all cores for their processing.
                    self.bus.send(msg)
                return True
            else:
                return False
        except nn.NanoMsgAPIError as e:
            return False

    def purge_inactive_cores( self, new_ts ):
        kill_list = []
        for key, coredata in self.cores.items():
            last_ts = coredata["ts_last_seen"]
            if (new_ts - last_ts) > remap_constants.THR_STATUS_DELAY:
                logger.info("Core %s missed a status report."%( key ))
            if (new_ts - last_ts) > remap_constants.MAX_STATUS_DELAY:
                logger.info("Core %s is considered dead."%( key ))
                kill_list.append( key )

        for key in kill_list:                
            del self.cores[ key ]

    # Request re-registration of existing core processes currently on the bus
    # allows failover restart of this node daemon.
    def req_registration( self ):
        msg = remap_utils.pack_msg( "node._plzreg.%s"%(self.nodeid), {} )
        self.bus.send( msg )

    # Some app initiator requests processing capacity
    def handle_showhands( self, recipientid, senderid, data ):
        avail, interruptable = self.hw.available_cpus( remap_utils.safe_get( data, "priority" ), self.cores )
        logger.info( "Evaluating app request: %s"%( senderid ) )
        if avail > 0 or interruptable > 0:
            logger.info( "Volunteering with %d cores, %d interruptable"%( avail, interruptable ))
            msg = remap_utils.pack_msg( "%s.raisehand.%s"%( senderid, self.nodeid ), {"cores":avail,"interruptable":interruptable} ) 
            self.forward_to_broker( msg )

    # Some app initiator wants this node to start work
    def handle_jobstart( self, recipientid, senderid, data ):
        avail, interruptable = self.hw.available_cpus( remap_utils.safe_get( data, "priority" ), self.cores )
        numcores = len(remap_utils.safe_get( data, "cores" ))
        if (avail + interruptable) >= numcores:
            logger.info("Starting job with %d cores"%( numcores ))
            self.hw.start_job( senderid, numcores, data )
        else:
            # Something changed in the meantime. Reject
            logger.info( "Initiator requested %d cores, %d can be committed. Rejecting"%( numcores, avail + interruptable ))
            msg = remap_utils.pack_msg( "%s.rejectjob.%s"%( senderid, self.nodeid ), {} ) 
            self.forward_to_broker( msg )

if __name__ == "__main__":
    logger.info("Starting node daemon")
    health_check = time.time()

    node = NodeDaemon()
    node.setup_bus()
    node.apply_timeouts()

    # wait 200ms to find broker, establish local connection
    time.sleep( 0.2 )

    # nanomsg doesn't event when a connection is lost
    # so we explicitly request reregistration of cores.
    node.req_registration()

    logger.info("Node daemon started")

    while( True ):
        try:
            while (node.process_bus_messages()):
                pass
            while (node.process_broker_messages()):
                pass
            if node.brokerChanged:
                node.setup_broker()
        except RemapException as re:
            logger.exception( re )

        # Every now and then check core heartbeats and remove cores no longer active.
        new_ts = time.time()
        if (new_ts - health_check) > remap_constants.HEALTH_CHECK_DELAY:
            health_check = new_ts            
            node.purge_inactive_cores( new_ts )

