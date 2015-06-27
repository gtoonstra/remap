import multiprocessing
import subprocess
import os

class NodeHardware(object):
    def __init__(self):
        self.waiting = []

    def available_cpus( self, priority, active_cores ):
        cpus = multiprocessing.cpu_count()
        interruptable = 0
        for key, coredata in active_cores.items():
            if coredata["priority"] < priority:
                interruptable = interruptable + 1
        available = cpus - 1 - len(active_cores)
        return available, interruptable

    def start_job( self, remaproot, jobid, numcores, data ):
        self.waiting = data["cores"]

        for i in range( 0, numcores ):
            coredata = data["cores"][i]
            jobid = None
            appdir = None
            if "jobid" in coredata:
                jobid = coredata["jobid"]
            if "appdir" in coredata:
                appdir = coredata["appdir"]

            if jobid == None or appdir == None:
                return False

            appdir = os.path.join( remaproot, "job", jobid, "app", appdir )

            env = os.environ
            pythonpath = ""
            if "PYTHONPATH" in env:
                pythonpath = env["PYTHONPATH"]
                pythonpath = pythonpath + ":" + appdir
            else:
                pythonpath = appdir
            env["PYTHONPATH"] = pythonpath

            path = ""
            thisdir = os.path.dirname( os.path.realpath(__file__) )
            coredir = os.path.abspath( os.path.join( thisdir, "..", "core" ) )
            path = path + coredir
            daemonfile = os.path.join( path, "core_daemon.py" )
            subprocess.Popen(["python3.4", daemonfile, remaproot], env=env)

        return True

    def grab_work_item( self ):
        if len(self.waiting) > 0:
            # Just grab any item
            return self.waiting.pop()
        return None


