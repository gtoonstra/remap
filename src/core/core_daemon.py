import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import logging
import time
import json

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException

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
    def __init__(self, remaproot):
        self.remaproot = remaproot
        self.pid = os.getpid()
        self.coreid = "unknown"
        self.sub = None
        self.pub = None
        self.jobid = None
        self.priority = 0
        self.ts_workRequested = 0
        self.keepWorking = True
        self.processed = 0
        self.total_size = 0
        self.input = None
        self.prevkey = None
        self.prevlist = None

    # The core daemon connects to the node first.
    def setup_node( self ):
        self.sub = nn.Socket( nn.SUB )
        self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "" )
        self.sub.connect("ipc:///tmp/node_sub.ipc")
        self.pub = nn.Socket( nn.PUB )
        self.pub.connect("ipc:///tmp/node_pub.ipc")

    def set_node_timeout( self, rcv_timeout ):
        # Apply a timeout for receiving messages from node.
        self.sub.set_int_option( nn.SOL_SOCKET, nn.RCVTIMEO, rcv_timeout )

    def process_node_messages( self ):
        try:
            msg = self.sub.recv()
            msgprefix, data = remap_utils.unpack_msg( msg )
            recipientid,msgtype,senderid = remap_utils.split_prefix(msgprefix)

            if recipientid == self.coreid:
                # This is directed at this core specifically, so it's more of a req/rep type
                self.process_personal_message( msgtype, senderid, data )
            elif recipientid == "global":
                self.process_global_message( msgtype, senderid, data )
            elif recipientid == "local":
                self.process_local_message( msgtype, senderid, data )    
            elif recipientid == "notlocal":
                self.process_global_message( msgtype, senderid, data )
            elif recipientid == "node":
                self.process_node_message( msgtype, senderid, data )
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
        self.pub.send( remap_utils.pack_msg( "node._hello.%d"%(self.pid), {"msgid":msgid,"pid":self.pid,"priority":self.priority} ) )

        # The while loop will terminate as soon as node stops sending messages,
        # so this should be safe to do.
        while True:
            try:
                msg = self.sub.recv()
                msgprefix, data = remap_utils.unpack_msg( msg )
                recipientid,msgtype,senderid = remap_utils.split_prefix(msgprefix)
                if msgtype != "_hey":
                    continue
                
                replymsgid = remap_utils.safe_get(data, "msgid")
                if replymsgid == msgid:
                    # this is us
                    self.coreid = remap_utils.safe_get(data, "coreid" )
                    self.sub.set_string_option( nn.SUB, nn.SUB_UNSUBSCRIBE, "" )
                    self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "global" )
                    self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "local" )
                    self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "notlocal" )
                    self.sub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.coreid )

                    logger.info( "Received coreid %s."%( self.coreid ))
                    return True
            except nn.NanoMsgAPIError as e:
                logger.error( "Node is currently not available." )
                break
        logger.error( "Registration failed" )
        return False

    def load_plugin(self, name):
        try:
            mod = __import__("module_%s" % name)
            return mod
        except ImportError as ie:
            raise RemapException( "No such worker type: %s"%( name ))

    def process_personal_message( self, msgtype, sender, workdata ):
        if msgtype == "_work":
            # {"appmodule": "wordcount", "appconfig": "wordcount/appconfig.json", "priority": 0, "inputfile": "gutenberg/picture-dorian-gray.txt", "type": "mapper", "jobid": "74d74370-1cca-11e5-afab-90e6ba78077a"}
            #
            # Data is the work to be executed
            # Prepare to start that work
            #
            logger.info("Received work item from node")

            self.jobid = workdata["jobid"]
            self.workertype = workdata["type"]
            self.appdir = workdata["appdir"]
            appconfig = None

            try:
                configfile = os.path.join( self.remaproot, "job", self.jobid, "app", workdata["appconfig"] )
                ap = open( configfile, 'r' )
                contents = ap.read()
                appconfig = json.loads( contents )
            except IOError as ioe:
                raise RemapException( "App config not found" ) from ioe

            if "module" not in appconfig:
                raise RemapException( "App config missing 'module' specification." )

            modulename = appconfig["module"]
            app = __import__(modulename, fromlist = ["*"])

            appconfig["remaproot"] = self.remaproot

            plugin = self.load_plugin( self.workertype )
            self.worker = plugin.create_worker( app, appconfig, workdata )
        else:
            logger.warn("Unknown personal message received from node: %s"%( msgtype ))

    def process_global_message( self, msgtype, sender, data ):
        pass

    def process_local_message( self, msgtype, sender, data ):
        pass

    def process_node_message( self, msgtype, sender, data ):
        if msgtype == "_plzreg":
            self.register()
        else:
            logger.warn("Unknown node message received from node: %s"%( msgtype ))

    def send_status( self ):
        if self.jobid != None:
            if not self.worker.module_manages_progress():
                data = self.worker.status()
                data["type"] = self.workertype
                self.pub.send( remap_utils.pack_msg( "%s.corestatus.%s"%(self.jobid, self.coreid), data ) )
            else:
                # Still need to send a message to node daemon, which manages processes at local level.
                self.pub.send( remap_utils.pack_msg( "node._status.%s"%(self.coreid), {} ) )

    def do_more_work( self ):
        # Check if we have some work to do already
        if self.jobid != None:
            if not self.worker.work():
                result, data = self.worker.result()
                data["type"] = self.workertype
                self.pub.send( remap_utils.pack_msg( "%s.%s.%s"%(self.jobid, result, self.coreid), data ) )
                return False
        else:
            # No work yet, so let's request some and otherwise wait 5 seconds until
            # we go away.
            if self.ts_workRequested > 0:
                if (time.time() - self.ts_workRequested) < 5:
                    # prevent loop with 100% cpu utilization
                    # wait at most 5 seconds for work to drop in.
                    time.sleep(0.1)
                    return True
                else:
                    logger.error("The work to be processed never arrived.")
                    return False

            logger.info( "Grabbing work item from node" )
            self.ts_workRequested = time.time()
            self.pub.send( remap_utils.pack_msg( "node._todo.%s"%(self.coreid), {} ) )
        return True

    def shutdown( self ):
        self.sub.close()
        self.pub.close()

if __name__ == "__main__":

    # Initialization of the core. We need a core id to work with (from node).
    core = CoreDaemon( sys.argv[1] )
    core.setup_node()

    # wait 50ms for node comms to be established
    time.sleep( 0.05 )

    # 5 attempts to register
    attempts = 5
    registered = False
    while ( attempts > 0 ):
        try:
            if core.register():
                registered = True
                break
            attempts = attempts - 1
        except RemapException as re:
            logger.exception( re )
            attempts = attempts - 1

    if not registered:
        logger.error( "Could not register with node to get a core id. Exiting." )
        sys.exit(-1)

    core.set_node_timeout( 0 )

    while( core.keepWorking ):
        try:
            while (core.process_node_messages()):
                pass
        except RemapException as re:
            logger.exception( re )

        try:
            if not core.do_more_work():
                logger.info("The core finished processing")
                core.keepWorking = False
                # allow time for any messages to be sent
                time.sleep( 0.1 )
            else:
                core.send_status()
        except RemapException as re:
            logger.exception( re )
            # take other actions

    core.shutdown()

