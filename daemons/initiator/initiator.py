import sys
import os
import nanomsg as nn
import logging
import time
import shutil
from monitor import Monitor
from threading import Timer
from planner import JobPlanner

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
        self.remaproot = rootdir
        self.broker_address = "unknown"
        self.brokerChanged = False
        self.bsub = None
        self.bpub = None
        self.bonjour = BonjourResolver( "_remap._tcp", self.cb_broker_changed )
        self.bonjour.start()
        self.jobid = remap_utils.unique_id()
        self.refreshed = 0
        self.job_in_progress = False
        self.rejectedtasks = {}
        self.completedtasks = {}
        self.last_check = time.time()

    def load_plugin(self, name):
        try:
            mod = __import__("module_%s" % name)
            return mod
        except ImportError as ie:
            raise RemapException( "No such worker type: %s"%( name ))

    # -------
    # Broker handling
    # -------

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
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "tracker")
        self.apply_timeouts()

        self.bpub = nn.Socket( nn.PUB )
        self.bpub.connect( "tcp://%s:8686"%( self.broker_address ))

        logger.info("Broker setup complete")

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

    # -------
    # Messaging handling
    # -------

    def update_corestatus( self, recipientid, senderid, data ):
        key = self.manager.get_work_key( data )
        if key in self.allocatedtasks:
            job = self.allocatedtasks[ key ]
            job["ts_finish"] = time.time() + 7

    def update_corecomplete( self, recipientid, senderid, data ):
        key = self.manager.get_work_key( data )
        logger.info( "Job %s completed."%( key ) )
        if key in self.allocatedtasks:
            job = self.allocatedtasks[ key ]
            task = self.tasks[ key ]
            self.completedtasks[ key ] = task
            del self.tasks[ key ]
            del self.allocatedtasks[ key ]
            logger.info( "%d tasks left, %d tasks committed, %d tasks complete, %d tasks failed."%( len(self.tasks), len(self.allocatedtasks), len(self.completedtasks), len(self.rejectedtasks) ))

    def update_hands( self, recipientid, senderid, data ):
        # "%s.raisehand.%s"%( senderid, self.nodeid ), {"cores":3,"interruptable":0}
        if senderid in self.nodes:
            self.nodes[ senderid ]["avail"] = data
        else:
            self.nodes[ senderid ] = {}
            self.nodes[ senderid ]["avail"] = data

    # -------
    # Job management
    # -------

    def start_job( self, jobdata ):
        if self.job_in_progress:
            raise RemapException("A job is currently in progress on this monitor")

        if "type" not in jobdata:
            raise RemapException("Must have job type specified" )
        if "priority" not in jobdata:
            raise RemapException("Must have priority specified" )
        if "parallellism" not in jobdata:
            raise RemapException("Must have parallellism specified" )

        self.jobtype = jobdata[ "type" ]
        self.priority = jobdata[ "priority" ]
        self.parallellism = jobdata[ "parallellism" ]
        plugin = self.load_plugin( self.jobtype )
        self.rejectedtasks = {}
        self.completedtasks = {}

        if self.jobid != None:
            self.bsub.set_string_option( nn.SUB, nn.SUB_UNSUBSCRIBE, self.jobid)

        if "jobid" in jobdata:
            self.jobid = jobdata["jobid"]
            del jobdata[ "jobid" ]
        else:
            self.jobid = remap_utils.unique_id()

        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.jobid)

        if "app" not in jobdata:
            raise RemapException( "The name of the app must be provided" )

        if jobdata["app"] not in self.list_apps():
            raise RemapException("No such application: %s"%(jobdata["app"]))

        config = {"jobid":self.jobid,"remaproot":self.remaproot}

        logger.info( "Started a new job: %s"%( self.jobid ))
        self.manager = plugin.create_manager( jobdata, config )

        if ((time.time() - self.refreshed) > 60):
            # Not refreshed > 60s
            self.refresh_nodes( self.priority )
            # Wait for a bunch of nodes to advertise themselves
            r = Timer(1.0, self.resume, ())
            r.start()
        else:
            self.resume()

    def resume( self ):
        self.manager.prepare()

        logger.info("Starting a %s job"%( self.jobtype ))

        self.planner = JobPlanner( self.manager.config_file )
        self.tasks = self.manager.plan_jobs( self.planner )

        logger.info( "Found %d tasks to execute"%( len(self.tasks) ))

        numnodes, self.allocatedtasks = self.planner.distribute_jobs_over_nodes( self.tasks, {}, self.nodes, self.parallellism )
        if len(self.allocatedtasks) == 0:
            logger.error("No nodes found to distribute the tasks.")
            self.job_in_progress = False
            return

        if self.manager.all_hands_on_deck():
            if len(self.allocatedtasks) != len(self.tasks):
                raise RemapException("Not enough cores available. Have %d, need %d."%( len(self.allocatedtasks), len(self.tasks) ))

        logger.info( "%d new tasks distributed over %d nodes."%( len(self.allocatedtasks), numnodes ))
        self.job_in_progress = True
        self.outbound_work( self.allocatedtasks )

    # In outbound work we update our local "jobs" data with timestamps
    # when they were sent out and send the task data to nodes.
    def outbound_work( self, jobs ):
        nodes = {}
        for key, job in jobs.items():
            nodeid = job["nodeid"]
            if nodeid in nodes:
                nodes[ nodeid ]["cores"].append( job["jobdata"] )
            else:
                tasks = {}
                tasklist = []
                job["ts_start"] = time.time()
                job["ts_finish"] = time.time() + 7
                tasklist.append( job["jobdata"] )
                tasks["cores"] = tasklist
                tasks["priority"] = self.priority
                nodes[ nodeid ] = tasks

        for nodeid, tasks in nodes.items():
            msg = remap_utils.pack_msg( "%s.jobstart.%s"%(nodeid,self.jobid), tasks )
            self.forward_to_broker( msg )

    def check_progress( self ):
        if self.manager.module_tracks_progress():
            if not self.manager.check_progress( len(self.tasks) ):
                self.manager.finish()
                self.job_in_progress = False
        else:
            if time.time() - self.last_check <= 4:
                return
            newtime = time.time()
            kill_list = []
            for key, job in self.allocatedtasks.items():
                if newtime > job["ts_finish"]:
                    # This job hasn't been updated, probably dead.
                    jobdata = job["jobdata"]
                    # Update tasks with an attempt + 1
                    task = self.tasks[ key ]
                    task["attempts" ] = task["attempts" ] + 1
                    nodeid = job["nodeid"]
                    logger.info( "Task %s failed on node %s. Reattempting elsewhere"%( key, nodeid ))
                    if task["attempts" ] > 4:
                        # 5 attempts so far. let's cancel it.
                        logger.warn("Task %s failed 5 attempts. Cancelling file to reject."%( key ))
                        del self.tasks[ key ]
                        kill_list.append( key )
                        self.rejectedtasks[ key ] = task

            for key in kill_list:
                del self.allocatedtasks[key]

            # Now also check if there are jobs that can be started
            if len(self.tasks) > 0:
                numnodes, new_allocations = self.planner.distribute_jobs_over_nodes( self.tasks, self.allocatedtasks, self.nodes, self.parallellism )
                if numnodes > 0:
                    logger.info( "%d new tasks distributed over %d nodes"%( len(new_allocations), numnodes ))
                    self.outbound_work( new_allocations )
                    self.allocatedtasks.update( new_allocations )

            if len(self.tasks) == 0 and len(self.allocatedtasks) == 0:
                # finished all work
                self.job_in_progress = False
                self.manager.finish()
                self.manager = None
                logger.info( "%d jobs left, %d jobs committed, %d jobs complete, %d jobs failed."%( len(self.tasks), len(self.allocatedtasks), len(self.completedtasks), len(self.rejectedtasks) ))

        self.last_check = time.time()        

    # -------
    # Node management
    # -------
    def refresh_nodes( self, priority ):
        self.nodes = {}
        self.priority = priority
        self.refreshed = time.time()
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

        if initiator.job_in_progress:
            initiator.check_progress()

