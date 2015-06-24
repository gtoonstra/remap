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
    def __init__(self):
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
        self.partprogress = 0.0
        self.input = None

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
            logger.info( "Received message from node: %s"%( msg ))
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
            # b'88750194067218_4516._work.88750194067218 {"inputfile": "/remote/data/tomsawyer.txt", "type": "mapper", "appconfig": "/remote/app/wordcount/appconfig.json", 
            # "outputdir": "/remote/im/jobid", "appmodule": "wordcount"}'
            #
            # Data is the work to be executed
            # Prepare to start that work
            #
            self.work = data
            self.jobid = self.work["jobid"]
            self.workertype = self.work["type"]
            self.appname = self.work["appmodule"]
            self.app = __import__(self.appname, fromlist = ["*"])
            try:
                ap = open( self.work["appconfig"], 'r' )
                contents = ap.read()
                self.appconfig = json.loads( contents )
            except IOError as ioe:
                raise AppException( "App config not found" ) from ioe

            if self.workertype == 'mapper':
                # This is a mapper operation
                self.input = self.app.create_mapper_reader( self.work["inputfile"] )
                self.outputdir = self.work["outputdir"]
                self.partitions = {}
            else:
                # This is a reducer operation
                self.reducerfiles = sorted(os.listdir( self.work["inputdir"] ))
                self.inputdir = self.work["inputdir"]
                self.numparts = len(self.reducerfiles)
                self.fraction = 100.0 / self.numparts
                self.completedparts = 0
                self.outputdir = self.work["outputdir"]
                self.partition = self.work["partition"]
                self.reducerWriter = self.app.create_reducer_writer( self.outputdir, self.partition )

    def process_global_message( self, msgtype, sender, data ):
        pass

    def process_local_message( self, msgtype, sender, data ):
        pass

    def process_node_message( self, msgtype, sender, data ):
        if msgtype == "_plzreg":
            self.register()

    def send_status( self ):
        if self.work != None:
            self.pub.send( remap_utils.pack_msg( "%s.status.%s"%(self.jobid, self.coreid), {"progress":self.progress} ) )

    def do_more_work( self ):
        # Check if we have some work to do already
        if self.work != None:
            if self.workertype == "mapper":
                self.mapper_work()
            else:
                self.reducer_work()
        else:
            # No work yet, so let's request some and otherwise wait 5 seconds until
            # we go away.
            if self.ts_workRequested > 0:
                if (time.time() - self.ts_workRequested) < 5:
                    # prevent loop with 100% cpu utilization
                    # wait at most 5 seconds for work to drop in.
                    time.sleep(0.1)
                    return
                else:
                    self.keepWorking = False
                    return

            logger.info( "Grabbing work item from node" )
            self.ts_workRequested = time.time()
            self.pub.send( remap_utils.pack_msg( "node._todo.%s"%(self.coreid), {} ) )

    # The work to be done as a mapper
    def mapper_work( self ):
        if self.input.isComplete():
            self.pub.send( remap_utils.pack_msg( "%s.complete.%s"%(self.jobid, self.coreid), {"inputfile":self.work["inputfile"]} ) )
            # allow time for message to be sent
            time.sleep( 0.5 )
            self.keepWorking = False
            return

        # so, do some work
        for k1, v1 in self.input.read():
            for part, k2, v2 in self.app.map( k1, v1 ):
                if part not in self.partitions:
                    self.partitions[ part ] = self.app.create_mapper_partitioner( self.outputdir, part, self.coreid )
                self.partitions[ part ].store( k2, v2 )

            p = self.input.progress()
            if p > self.progress+1:
                self.progress = int(p)
                break

        if self.input.isComplete():
            self.progress = 100
            self.input.close()
            for part in self.partitions:
                self.partitions[part].sort_flush_close()
        self.send_status()

    # The work to be done as a reducer
    def reducer_work( self ):
        if self.input == None:
            if len(self.reducerfiles) == 0:
                self.pub.send( remap_utils.pack_msg( "%s.complete.%s"%(self.jobid, self.coreid), {"partition":self.work["partition"]} ) )
                # allow time for message to be sent
                time.sleep( 0.5 )
                self.keepWorking = False
                return
            self.input = self.app.create_reducer_reader( os.path.join( self.inputdir, self.reducerfiles.pop(0) ))

        for key,list_of_values in self.input.read():
            for k3,v3 in self.app.reduce( key, list_of_values ):
                self.reducerWriter.store( k3, v3 )

            p = self.input.progress()
            if p > self.partprogress + 0.01:
                self.partprogress = p
                self.progress = self.partprogress * self.fraction + (self.completedparts * self.fraction)
                break

        if self.input.isComplete():
            self.completedparts = self.completedparts + 1
            self.progress = (self.completedparts * self.fraction)
            self.input.close()
            self.partprogress = 0
            self.input = None

        self.send_status()

    def shutdown( self ):
        self.sub.close()
        self.pub.close()

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
            core.do_more_work()
        except RemapException as re:
            logger.exception( re )
            # take other actions

    core.shutdown()

