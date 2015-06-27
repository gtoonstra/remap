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
        self.jobid = None
        self.bonjour = BonjourResolver( "_remap._tcp", self.cb_broker_changed )
        self.bonjour.start()
        self.jobid = remap_utils.unique_id()
        self.priority = 0
        self.refreshed = time.time()
        self.job_in_progress = False
        self.rejectedjobs = {}
        self.completedjobs = {}
        self.last_check = time.time()
        self.phase = "mapper"

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
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "tracker")
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
        if data["type"] == "mapper":
            inputfile = data["inputfile"]
            if inputfile in self.allocatedjobs:
                job = self.allocatedjobs[ inputfile ]
                job["ts_finish"] = time.time() + 7

    def update_corecomplete( self, recipientid, senderid, data ):
        if data["type"] == "mapper":
            inputfile = data["inputfile"]
            logger.info( "Job %s completed."%( inputfile ) )
            if inputfile in self.allocatedjobs:
                job = self.allocatedjobs[ inputfile ]
                if job["jobdata"]["inputfile"] == inputfile:
                    mapperjob = self.mapperjobs[ inputfile ]
                    self.completedjobs[ inputfile ] = mapperjob
                    del self.mapperjobs[ inputfile ]
                    del self.allocatedjobs[ inputfile ]
                    logger.info( "%d jobs left, %d jobs committed, %d jobs complete, %d jobs failed."%( len(self.mapperjobs), len(self.allocatedjobs), len(self.completedjobs), len(self.rejectedjobs) ))
        if data["type"] == "reducer":
            partition = data["partition"]
            logger.info( "Job %s completed."%( partition ) )
            if partition in self.allocatedjobs:
                job = self.allocatedjobs[ partition ]
                if job["jobdata"]["partition"] == partition:
                    reducerjob = self.reducerjobs[ partition ]
                    self.completedjobs[ partition ] = reducerjob
                    del self.reducerjobs[ partition ]
                    del self.allocatedjobs[ partition ]
                    logger.info( "%d jobs left, %d jobs committed, %d jobs complete, %d jobs failed."%( len(self.reducerjobs), len(self.allocatedjobs), len(self.completedjobs), len(self.rejectedjobs) ))

    def update_hands( self, recipientid, senderid, data ):
        # "%s.raisehand.%s"%( senderid, self.nodeid ), {"cores":3,"interruptable":0}
        if senderid in self.nodes:
            self.nodes[ senderid ]["avail"] = data
        else:
            self.nodes[ senderid ] = {}
            self.nodes[ senderid ]["avail"] = data

    def start_mapper_job( self, appname, priority, inputdir, outputdir, parallellism ):
        if self.job_in_progress:
            raise RemapException("A job is currently in progress on this monitor")

        self.phase = "mapper"

        if self.jobid != None:
            # unsubscribe from old.
            self.bsub.set_string_option( nn.SUB, nn.SUB_UNSUBSCRIBE, self.jobid)

        # subscribe to new
        self.jobid = remap_utils.unique_id()
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.jobid)

        if appname not in self.list_apps():
            raise RemapException("No such application: %s"%(appname))

        self.appname = appname
        self.parallellism = parallellism
        self.inputdir = os.path.join( self.datadir, inputdir.strip("/") )

        if not os.path.isdir( self.inputdir ):
            raise RemapException("Input dir does not exist: %s"%(self.inputdir))

        self.relinputdir = inputdir
        self.reloutputdir = outputdir

        if priority != self.priority or ((time.time() - self.refreshed) > 60):
            # Not same priority or refreshed > 60s
            self.refresh_nodes( self.priority )
            # Wait for the network to be refreshed, so we work with latest data
            r = Timer(1.0, self.resume_mapper_job_start, ())
            r.start()
        else:
            self.resume_mapper_job_start()

        return "OK"

    def resume_mapper_job_start( self ):
        logger.info("Starting a mapper job: %s"%( self.appname ))

        self.app_dir = os.path.join( self.appsdir, self.appname )
        self.job_dir = os.path.join( self.jobsdir, self.jobid )
        self.app_job_dir = os.path.join( self.job_dir, "app", self.appname )
        self.partitions_dir = os.path.join( self.job_dir, "part" )
        self.config_file = os.path.join( self.app_job_dir, "appconfig.json" )
        self.relconfig_file = os.path.join( self.appname, "appconfig.json" )

        os.makedirs( self.job_dir )
        # app job dir created by copytree
        # os.makedirs( self.app_job_dir )
        os.makedirs( self.partitions_dir )
        
        try:
            shutil.copytree(self.app_dir, self.app_job_dir)
            # Directories are the same
        except shutil.Error as e:
            raise RemapException("Directory not copied: %s"%(e))
        except OSError as e:
            raise RemapException("Directory not copied: %s"%(e))

        self.planner = JobPlanner( self.jobid, self.appname, self.config_file, self.relconfig_file, self.inputdir, self.relinputdir, None, None )
        self.mapperjobs = self.planner.define_mapper_jobs( self.priority )
        logger.info( "Found %d mapper jobs to execute"%( len(self.mapperjobs) ))

        numnodes, self.allocatedjobs = self.planner.distribute_jobs_over_nodes( self.mapperjobs, {}, self.nodes, self.parallellism )
        if len(self.allocatedjobs) == 0:
            logger.error("No nodes found to distribute the tasks.")
            self.job_in_progress = False
            return

        logger.info( "%d new tasks distributed over %d nodes."%( len(self.allocatedjobs), numnodes ))
        self.job_in_progress = True
        self.outbound_work( self.allocatedjobs )

    def start_reducer_job( self, appname, priority, jobid, outputdir, parallellism ):
        if self.job_in_progress:
            raise RemapException("A job is currently in progress on this monitor")

        if appname not in self.list_apps():
            raise RemapException("No such application: %s"%(appname))

        self.phase = "reducer"

        self.appname = appname
        self.parallellism = parallellism
        self.jobid = jobid
        self.bsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, self.jobid)

        verifyDir = os.path.join( self.remaproot, "job", jobid )
        self.outputdir = os.path.join( self.datadir, outputdir.strip("/") )

        if not os.path.isdir( verifyDir ):
            raise RemapException("Input dir does not exist: %s"%(verifyDir))
        if os.path.isdir( self.outputdir ):
            raise RemapException("Output dir already exists: %s"%(self.outputdir))

        os.makedirs( self.outputdir )

        self.reloutputdir = outputdir

        if priority != self.priority or ((time.time() - self.refreshed) > 60):
            # Not same priority or refreshed > 60s
            self.refresh_nodes( self.priority )
            # Wait for the network to be refreshed, so we work with latest data
            r = Timer(1.0, self.resume_reducer_job_start, ())
            r.start()
        else:
            self.resume_reducer_job_start()

        return "OK"

    def resume_reducer_job_start( self ):
        logger.info("Starting a reducer job: %s"%( self.appname ))

        self.app_dir = os.path.join( self.appsdir, self.appname )
        self.job_dir = os.path.join( self.jobsdir, self.jobid )
        self.app_job_dir = os.path.join( self.job_dir, "app", self.appname )
        self.partitions_dir = os.path.join( self.job_dir, "part" )
        self.config_file = os.path.join( self.app_job_dir, "appconfig.json" )
        self.relconfig_file = os.path.join( self.appname, "appconfig.json" )
       
        try:
            shutil.copytree(self.app_dir, self.app_job_dir)
            # Directories are the same
        except shutil.Error as e:
            raise RemapException("Directory not copied: %s"%(e))
        except OSError as e:
            pass

        self.planner = JobPlanner( self.jobid, self.appname, self.config_file, self.relconfig_file, self.remaproot, None, self.outputdir, self.reloutputdir )
        self.reducerjobs = self.planner.define_reducer_jobs( self.priority )
        logger.info( "Found %d reducer jobs to execute"%( len(self.reducerjobs) ))

        numnodes, self.allocatedjobs = self.planner.distribute_jobs_over_nodes( self.reducerjobs, {}, self.nodes, self.parallellism )
        if len(self.allocatedjobs) == 0:
            logger.error("No nodes found to distribute the tasks.")
            self.job_in_progress = False
            return

        logger.info( "%d new tasks distributed over %d nodes."%( len(self.allocatedjobs), numnodes ))
        self.job_in_progress = True
        self.outbound_work( self.allocatedjobs )

    def outbound_work( self, jobs ):
        nodes = {}
        for inputfile, job in jobs.items():
            nodeid = job["nodeid"]
            if nodeid in nodes:
                nodes[ nodeid ]["cores"].append( job["jobdata"] )
            else:
                tasks = {}
                tasks["priority"] = self.priority
                tasklist = []
                job["ts_start"] = time.time()
                job["ts_finish"] = time.time() + 7
                tasklist.append( job["jobdata"] )
                tasks["cores"] = tasklist
                nodes[ nodeid ] = tasks

        for nodeid, tasks in nodes.items():
            msg = remap_utils.pack_msg( "%s.jobstart.%s"%(nodeid,self.jobid), tasks )
            self.forward_to_broker( msg )

    def check_progress( self ):
        # corejobs are jobs in progress that actually run
        # mapperjobs are jobs to be done
        # In progress we simply check 

        if time.time() - self.last_check <= 4:
            return

        if self.phase == "mapper":
            self.check_progress_mapper()
        if self.phase == "reducer":
            self.check_progress_reducer()

        self.last_check = time.time()        

    def check_progress_mapper(self):
        newtime = time.time()
        kill_list = []
        for inputfile, job in self.allocatedjobs.items():
            if newtime > job["ts_finish"]:
                # This job hasn't been updated, probably dead.
                jobdata = job["jobdata"]
                if jobdata["type"] == "mapper":
                    # this is a mapper job. Update mapperjobs with an attempt + 1
                    mapperjob = self.mapperjobs[ inputfile ]
                    mapperjob["attempts" ] = mapperjob["attempts" ] + 1
                    nodeid = job["nodeid"]
                    logger.info( "Input file %s failed on node %s. Reattempting elsewhere"%( inputfile, nodeid ))
                    if mapperjob["attempts" ] > 4:
                        # 5 attempts so far. let's cancel it.
                        logger.warn("Input file %s failed 5 attempts. Cancelling file to reject."%( inputfile ))
                        del self.mapperjobs[ inputfile ]
                        kill_list.append( inputfile )
                        self.rejectedjobs[ inputfile ] = mapperjob

        for inputfile in kill_list:
            del self.allocatedjobs[inputfile]

        # Now also check if there are jobs that can be started
        if len(self.mapperjobs) > 0:
            numnodes, new_allocations = self.planner.distribute_jobs_over_nodes( self.mapperjobs, self.allocatedjobs, self.nodes, self.parallellism )
            if numnodes > 0:
                logger.info( "%d new tasks distributed over %d nodes"%( len(new_allocations), numnodes ))
                self.outbound_work( new_allocations )
                self.allocatedjobs.update( new_allocations )

        if len(self.mapperjobs) == 0 and len(self.allocatedjobs) == 0:
            # finished mappers
            self.job_in_progress = False
            logger.info( "%d jobs left, %d jobs committed, %d jobs complete, %d jobs failed."%( len(self.mapperjobs), len(self.allocatedjobs), len(self.completedjobs), len(self.rejectedjobs) ))

    def check_progress_reducer( self ):
        newtime = time.time()
        kill_list = []
        for partition, job in self.allocatedjobs.items():
            if newtime > job["ts_finish"]:
                # This job hasn't been updated, probably dead.
                jobdata = job["jobdata"]
                if jobdata["type"] == "reducer":
                    # this is a reducer job. Update reducerjobs with an attempt + 1
                    if partition in self.reducerjobs:
                        reducerjob = self.reducerjobs[ partition ]
                        reducerjob["attempts" ] = reducerjob["attempts" ] + 1
                        nodeid = job["nodeid"]
                        logger.info( "Input directory %s failed on node %s. Reattempting elsewhere"%( partition, nodeid ))
                        if reducerjob["attempts" ] > 4:
                            # 5 attempts so far. let's cancel it.
                            logger.warn("Partition %s failed 5 attempts. Cancelling file to reject."%( partition ))
                            del self.reducerjobs[ partition ]
                            kill_list.append( partition )
                            self.rejectedjobs[ partition ] = reducerjob

        for partition in kill_list:
            del self.allocatedjobs[partition]

        # Now also check if there are jobs that can be started
        if len(self.reducerjobs) > 0:
            numnodes, new_allocations = self.planner.distribute_jobs_over_nodes( self.reducerjobs, self.allocatedjobs, self.nodes, self.parallellism )
            if numnodes > 0 and len(new_allocations) > 0:
                logger.info( "%d new tasks distributed over %d nodes"%( len(new_allocations), numnodes ))
                self.outbound_work( new_allocations )
                self.allocatedjobs.update( new_allocations )

        if len(self.reducerjobs) == 0 and len(self.allocatedjobs) == 0:
            # finished mappers
            self.phase = "finish"
            self.job_in_progress = False
            logger.info( "%d jobs left, %d jobs committed, %d jobs complete, %d jobs failed."%( len(self.reducerjobs), len(self.allocatedjobs), len(self.completedjobs), len(self.rejectedjobs) ))

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

