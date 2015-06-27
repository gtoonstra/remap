import os
import sys
import logging

import json
import math
import time

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

from lib.bonjour_detect import BonjourResolver
import lib.remap_utils as remap_utils
import lib.remap_constants as remap_constants
from lib.remap_utils import RemapException

logging.basicConfig( level=logging.INFO ) 

# logger = logging.getLogger(__name__)
logger = logging.getLogger("Initiator")

class JobPlanner(object):
    def __init__(self, jobid, app, config_file, relconfig_file, inputdir, relinputdir, outputdir, reloutputdir):
        self.jobid = jobid
        self.app = app
        self.appconfig = relconfig_file

        f = open(config_file)
        data = f.read()
        self.config = json.loads(data)
        self.inputdir = inputdir
        self.relinputdir = relinputdir
        self.outputdir = outputdir
        self.reloutputdir = reloutputdir

    def define_mapper_jobs( self, priority ):
        # First, let's just generate a list of jobs
        mapperjobs = {}

        # Grab all input files
        files = [f for f in os.listdir( self.inputdir ) if os.path.isfile(os.path.join(self.inputdir,f))]

        logger.info("Found %d files to process in %s"%( len(files), self.inputdir ))

# nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"wordcount/appconfig.json","type":"mapper","inputfile":"gutenberg/tomsawyer.txt"}]}'

# nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"wordcount/appconfig.json","type":"reducer","outputdir":"wordscounted","partition":"_default"}]}'

        # ( Here app config probably tells us how to split files. Not doing that for now. Just process whole thing )
        for f in files:
            inputfile = os.path.join( self.relinputdir, f )
            job = { "inputfile": inputfile }
            job["jobid"] = self.jobid
            job["priority"] = priority
            job["appdir"] = self.app
            job["appconfig"] = self.appconfig
            job["type"] = "mapper"
            mapperjobs[ inputfile ] = { "attempts": 0, "job": job }

        # Can't do anything for reducers yet, because this depends on number of partitions and mappers have
        # to finish anyway prior to running reducers
        return mapperjobs

    def define_reducer_jobs( self, priority ):
        # First, let's just generate a list of jobs
        reducerjobs = {}

        parts_dir = os.path.join( self.inputdir, "job", self.jobid, "part" )

        # Grab all input files
        dirs = [d for d in os.listdir( parts_dir ) if os.path.isdir(os.path.join(parts_dir,d))]
        logger.info("Found %d partitions to process in %s"%( len(dirs), parts_dir ))

# nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"wordcount/appconfig.json","type":"mapper","inputfile":"gutenberg/tomsawyer.txt"}]}'

# nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"wordcount/appconfig.json","type":"reducer","outputdir":"wordscounted","partition":"_default"}]}'

        # ( Here app config probably tells us how to split files. Not doing that for now. Just process whole thing )
        for d in dirs:
            job = {}
            job["jobid"] = self.jobid
            job["partition"] = d
            job["priority"] = priority
            job["appdir"] = self.app
            job["appconfig"] = self.appconfig
            job["type"] = "reducer"
            job["outputdir"] = self.reloutputdir
            reducerjobs[ d ] = { "attempts": 0, "job": job }

        # Can't do anything for reducers yet, because this depends on number of partitions and mappers have
        # to finish anyway prior to running reducers
        return reducerjobs

    def distribute_jobs_over_nodes( self, availjobs, allocatedjobs, nodes, parallellism ):
        # Making a copy first, it gets modified
        availjobs = dict(availjobs)
        corejobs = {}
        committed = {}

        for inputfile, job in allocatedjobs.items():
            nodeid = job["nodeid"]
            if nodeid in committed:
                committed[ nodeid ] = committed[ nodeid ]+1
            else:
                committed[ nodeid ] = 1                

        # Figure out how to distribute mappers.
        numcores = 0
        numint = 0
        for key in nodes:
            numcores = numcores + nodes[key]["avail"]["free"]
            numint = numint + nodes[key]["avail"]["interruptable"]

        parallels = min( numcores, parallellism )

        added = True
        while len(availjobs) > 0 and added:
            i = 0
            added = False
            for key in nodes:
                if key not in committed:
                    committed[ key ] = 0

                avail = nodes[key]["avail"]["free"] - committed[key]
                if avail <= 0:
                    break
                if i == parallels:
                    break
                if len(availjobs)==0:
                    break

                for j in range( 0, avail ):
                    if i == parallels:
                       break
                    i = i + 1

                    if len(availjobs)>0:
                        workfile, data = availjobs.popitem()
                        corejobs[ workfile ] = {}
                        corejobs[ workfile ]["jobdata"] = data["job"]
                        corejobs[ workfile ]["nodeid"] = key
                        corejobs[ workfile ]["ts_start"] = time.time()
                        corejobs[ workfile ]["ts_finish"] = time.time() + 7
                        committed[ key ] = committed[ key ] + 1
                        added = True
                    else:
                        break

        return len(committed), corejobs

