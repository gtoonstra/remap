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
    def __init__(self, config_file):
        f = open(config_file)
        data = f.read()
        self.config = json.loads(data)

    def task_per_file_in_dir( self, job_def_cb, input_dir ):
        tasks = {}
        files = [f for f in os.listdir( input_dir ) if os.path.isfile(os.path.join(input_dir,f))]
        logger.info("Found %d files to process in %s"%( len(files), input_dir  ))

        ctr = 0
        for f in files:
            key, jobdata = job_def_cb( f, ctr )
            tasks[ key ] = { "attempts": 0, "jobdata": jobdata }
            ctr = ctr + 1

        return tasks

    def task_per_dir( self, job_def_cb, input_dir ):
        # First, let's just generate a list of jobs
        tasks = {}

        # Grab all input files
        dirs = [d for d in os.listdir( input_dir ) if os.path.isdir(os.path.join(input_dir,d))]
        logger.info("Found %d partitions to process in %s"%( len(dirs), input_dir ))

        # ( Here app config probably tells us how to split files. Not doing that for now. Just process whole thing )
        for d in dirs:
            key, jobdata = job_def_cb( d )
            tasks[ key ] = { "attempts": 0, "jobdata": jobdata }

        return tasks

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
                        if workfile in allocatedjobs:
                            continue
                        corejobs[ workfile ] = {}
                        corejobs[ workfile ]["jobdata"] = data["jobdata"]
                        corejobs[ workfile ]["nodeid"] = key
                        corejobs[ workfile ]["ts_start"] = time.time()
                        corejobs[ workfile ]["ts_finish"] = time.time() + 7
                        committed[ key ] = committed[ key ] + 1
                        added = True
                    else:
                        break

        return len(committed), corejobs

