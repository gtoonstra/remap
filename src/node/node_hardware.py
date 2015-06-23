import multiprocessing

class NodeHardware(object):
    def __init__(self):
        pass
    
    def available_cpus( self, priority, active_cores ):
        cpus = multiprocessing.cpu_count()
        uninterruptable = len(active_cores)
        for key, coredata in active_cores.items():
            if coredata["priority"] < priority:
                uninterruptable = uninterruptable - 1
        available = cpus - 1 - uninterruptable
        return available

