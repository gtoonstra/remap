import multiprocessing
import subprocess
import os

class NodeHardware(object):
    def __init__(self):
        self.waiting = {}

    def available_cpus( self, priority, active_cores ):
        cpus = multiprocessing.cpu_count()
        interruptable = 0
        for key, coredata in active_cores.items():
            if coredata["priority"] < priority:
                interruptable = interruptable + 1
        available = cpus - 1 - len(active_cores)
        return available, interruptable

    def start_job( self, jobid, numcores, data ):
        self.waiting = data["cores"]
        
        for i in range( 0, numcores ):
            env = os.environ
            path = ""
            thisdir = os.path.dirname( os.path.realpath(__file__) )
            coredir = os.path.abspath( os.path.join( thisdir, "..", "core" ) )
            path = path + coredir
            daemonfile = os.path.join( path, "core_daemon.py" )
            subprocess.Popen(["python3.4", daemonfile], env=env)

