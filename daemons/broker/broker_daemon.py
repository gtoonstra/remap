import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import pybonjour
import logging
import time
from bonjour_register import BonjourRegistration

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("BrokerDaemon")

if __name__ == "__main__":
    # Interactions between brokers not implemented yet, probably requires C
    # and 'poll' functionality to look for any messages.
    # 
    # Which means that brokers do not need to know about connected clients yet...
    # 
    #bpub = nn.socket( nn.PUB )
    #bpub.bind( "tcp://0.0.0.0:8787" )

    logger.info( "Starting Broker" )

    # Local pub and sub
    lpub = nn.Socket( nn.PUB, domain=nn.AF_SP_RAW )
    lpub.bind( "tcp://0.0.0.0:8687" )
    lsub = nn.Socket( nn.SUB, domain=nn.AF_SP_RAW )
    lsub.bind( "tcp://0.0.0.0:8686" )
    lsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "")

    # expose this service over bonjour
    bv = BonjourRegistration( "vertexbroker", "_vertexremap._tcp", 8689 )
    bv.start()

    time.sleep( 1 )

    br = BonjourRegistration( "broker", "_remap._tcp", 8687 )
    br.start()

    # move messages between them
    dev = nn.Device( lsub, lpub )
    logger.info( "Broker started" )
    dev.start()

