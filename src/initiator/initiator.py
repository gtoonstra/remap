import sys
import os
import nanomsg as nn
import logging
import time
from monitor import Monitor

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

from lib.bonjour_detect import BonjourResolver
import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException
import http_interface

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Initiator")

class Initiator( Monitor ):
    def __init__(self, rootdir):
        Monitor.__init__(self, rootdir)
        self.broker_address = "unknown"
        self.brokerChanged = False
        self.bsub = None
        self.bpub = None
        self.jobid = None
        self.bonjour = BonjourResolver( "_remap._tcp", self.cb_broker_changed )
        self.bonjour.start()
        self.jobid = remap_utils.unique_id()
        self.priority = 0

    def apply_timeouts( self ):
        if self.bsub != None:
            rcv_timeout = 100
            self.bsub.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, rcv_timeout )

    def cb_broker_changed( self, broker_address ):
        logger.info("Received new broker address: %s"%(broker_address) )
        self.broker_address = broker_address
        self.brokerChanged = True

    def forward_to_broker( self, msg ):
        if self.bpub != None:
            try:
                self.bpub.send( msg )
            except nn.NanoMsgAPIError as e:
                pass

    def setup_broker( self ):
        self.brokerChanged = False
        if self.bsub != None:
            self.bsub.close()
            self.bsub = None

        self.apply_timeouts()

        if self.broker_address == "unknown":
            logger.error("Deferring broker setup as address is still unknown.")
            return

        self.bsub = nn.Socket( nn.SUB )
        self.bsub.connect( "tcp://%s:8687"%( self.broker_address ))
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "global")
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "local")
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "notlocal")
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.jobid)
        self.apply_timeouts()

        self.bpub = nn.Socket( nn.PUB )
        self.bpub.connect( "tcp://%s:8686"%( self.broker_address ))

        logger.info("Broker setup complete")

    def process_broker_messages( self ):
        if self.bsub == None:
            # No broker is known yet.
            if self.brokerChanged:
                logger.info("The broker configuration changed.")
                self.setup_broker()
                if self.bsub == None:
                    logger.info("Failed broker setup.")
                    return False
            else:              
                return False

        try:
            # Grab next msg from broker if any
            msg = self.bsub.recv()
            if msg != None and len(msg)>0:
                msgprefix, data = remap_utils.unpack_msg( msg )
                logger.info("Received %s"%(msgprefix))
                recipientid,msgtype,senderid = remap_utils.split_prefix(msgprefix)
                if msgtype == "complete":
                    self.update_corecomplete( recipientid, senderid, data )
                if msgtype == "corestatus":
                    self.update_corestatus( recipientid, senderid, data )
                if msgtype == "raisehand":
                    self.update_hands( recipientid, senderid, data )
                return True
            else:
                return False
        except nn.NanoMsgAPIError as e:
            return False

    def update_corestatus( self, recipientid, senderid, data ):
        if recipientid == self.jobid:
            if senderid in self.cores:
                self.cores[ senderid ][ "status" ] = data

    def update_corecomplete( self, recipientid, senderid, data ):
        if recipientid == self.jobid:
            if senderid in self.cores:
                self.cores[ senderid ][ "complete" ] = True

    def update_hands( self, recipientid, senderid, data ):
        if recipientid == self.jobid:
            if senderid in self.nodes:
                self.nodes[ senderid ]["hands"] = data
            else:
                self.nodes[ senderid ] = {}
                self.nodes[ senderid ]["hands"] = data


    def start_new_job( self, appname ):
        if self.jobid != None:
            # unsubscribe from old.
            self.bsub.set_string_option( nn.SUB, nn.SUB_UNSUBSCRIBE, self.jobid)

        # subscribe to new
        self.jobid = remap_utils.unique_id()
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.jobid)

    def refresh_nodes( self ):
        msg = remap_utils.pack_msg( "local.showhands.%s"%(self.jobid), {"priority":self.priority} )
        self.forward_to_broker( msg )

if __name__ == "__main__":
    logger.info("Starting initiator daemon")

    if ( len(sys.argv) < 2 ):
        print("Must supply one argument, the 'rootdir'")
        sys.exit(-1)

    initiator = Initiator( sys.argv[1])
    initiator.apply_timeouts()

    http_interface.start( initiator )

    # wait 200ms to find broker, establish local connection
    time.sleep( 0.2 )

    logger.info("Initiator started")

    while( True ):
        try:
            while (initiator.process_broker_messages()):
                pass
            if initiator.brokerChanged:
                initiator.setup_broker()
        except RemapException as re:
            logger.exception( re )

