import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import logging
import time
import json
import heapq
from operator import itemgetter

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
        self.appid = "unknown"
        self.jobid = "unknown"
        self.priority = 0
        self.work = None
        self.ts_workRequested = 0
        self.keepWorking = True
        self.workertype = None
        self.progress = 0
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

    def process_personal_message( self, msgtype, sender, data ):
        if msgtype == "_work":
            # {"appmodule": "wordcount", "appconfig": "wordcount/appconfig.json", "priority": 0, "inputfile": "gutenberg/picture-dorian-gray.txt", "type": "mapper", "jobid": "74d74370-1cca-11e5-afab-90e6ba78077a"}
            #
            # Data is the work to be executed
            # Prepare to start that work
            #
            logger.info("Received work item from node")
            self.work = data
            self.jobid = self.work["jobid"]
            self.workertype = self.work["type"]
            self.appdir = self.work["appdir"]

            try:
                configfile = os.path.join( self.remaproot, "job", self.jobid, "app", self.work["appconfig"] )
                ap = open( configfile, 'r' )
                contents = ap.read()
                self.appconfig = json.loads( contents )
            except IOError as ioe:
                raise RemapException( "App config not found" ) from ioe

            if "module" not in self.appconfig:
                raise RemapException( "App config missing 'module' specification." )

            modulename = self.appconfig["module"]
            self.app = __import__(modulename, fromlist = ["*"])

            if self.workertype == 'mapper':
                # This is a mapper operation
                inputfile = os.path.join( self.remaproot, "data", self.work["inputfile"] )
                outputdir = os.path.join( self.remaproot, "job", self.jobid, "part" )

                self.input = self.app.create_mapper_reader( inputfile )
                self.outputdir = outputdir
                self.partitions = {}
            else:
                # This is a reducer operation
                inputdir = os.path.join( self.remaproot, "job", self.jobid, "part", self.work["partition"] )
                outputdir = os.path.join( self.remaproot, self.work["outputdir"] )

                self.reducerfiles = sorted(os.listdir( inputdir ))
                self.inputdir = inputdir
                self.numparts = len(self.reducerfiles)
                self.fraction = 100.0 / self.numparts
                self.completedparts = 0
                self.outputdir = outputdir
                self.partition = self.work["partition"]
                self.reducerWriter = self.app.create_reducer_writer( self.outputdir, self.partition )

                self.sources = []
                for filename in self.reducerfiles:
                    f = self.app.create_reducer_reader( os.path.join( self.inputdir, filename ))
                    self.sources.append( f )
                    self.total_size = self.total_size + f.filesize

                decorated = [
                    ((key,list_of_values,recsize) for key,list_of_values,recsize in f.read())
                    for f in self.sources]
                self.merged = heapq.merge(*decorated)
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
        if self.work != None:
            if self.workertype == "mapper":
                self.pub.send( remap_utils.pack_msg( "%s.corestatus.%s"%(self.jobid, self.coreid), {"type":self.workertype,"inputfile":self.work["inputfile"],"progress":self.progress} ) )
            else:
                self.pub.send( remap_utils.pack_msg( "%s.corestatus.%s"%(self.jobid, self.coreid), {"type":self.workertype,"progress":self.progress} ) )

    def do_more_work( self ):
        # Check if we have some work to do already
        if self.work != None:
            if self.workertype == "mapper":
                return self.mapper_work()
            else:
                return self.reducer_work()
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
                    return False

            logger.info( "Grabbing work item from node" )
            self.ts_workRequested = time.time()
            self.pub.send( remap_utils.pack_msg( "node._todo.%s"%(self.coreid), {} ) )
        return True

    # The work to be done as a mapper
    def mapper_work( self ):
        if self.input != None:
            if self.input.isComplete():
                self.pub.send( remap_utils.pack_msg( "%s.complete.%s"%(self.jobid, self.coreid), {"inputfile":self.work["inputfile"],"type":"mapper"} ) )
                # allow time for message to be sent
                time.sleep( 0.5 )
                self.keepWorking = False
                return True

            # so, do some work
            for k1, v1 in self.input.read():
                for part, k2, v2 in self.app.map( k1, v1 ):
                    if part not in self.partitions:
                        self.partitions[ part ] = self.app.create_mapper_partitioner( self.outputdir, part, self.coreid )
                    self.partitions[ part ].store( k2, v2 )

                p = self.input.progress()
                if p > self.progress+5:
                    self.progress = int(p)
                    break

            if self.input.isComplete():
                self.progress = 100
                self.input.close()
                for part in self.partitions:
                    self.partitions[part].sort_flush_close()
            self.send_status()
        return True

    # The work to be done as a reducer
    def reducer_work( self ):
        if len(self.sources) == 0:
            self.pub.send( remap_utils.pack_msg( "%s.complete.%s"%(self.jobid, self.coreid), {"inputdir":self.work["inputdir"]} ) )
            # allow time for message to be sent
            time.sleep( 0.5 )
            self.keepWorking = False
            return

        readrec = False
        for k2,v2,recsize in self.merged:
            readrec = True
            if self.prevkey == None:
                # Initialize the very first step
                self.prevkey = k2
                self.prevlist = v2
                self.processed = recsize
            elif self.prevkey != k2:
                # The key changed. Dump all values of previous step
                for k3,v3 in self.app.reduce( self.prevkey, self.prevlist ):
                    self.reducerWriter.store( k3, v3 )
                self.prevkey = k2
                self.prevlist = v2
                self.processed = self.processed + recsize
            else:
                # Add another record to the list
                self.prevlist = self.prevlist + v2
                self.processed = self.processed + recsize

            p = (self.processed / self.total_size) * 100
            if p > self.progress+5:
                self.progress = int(p)
                # breaking out of the loop to check up on messages
                break

        if not readrec:
            # done
            self.progress = 100
            for f in self.sources:
                f.close()
            self.sources = []
            self.reducerWriter.close()

        self.send_status()
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
                logger.error("The core failed to process work")
                core.keepWorking = False
        except RemapException as re:
            logger.exception( re )
            # take other actions

    core.shutdown()

