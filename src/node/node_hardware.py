import multiprocessing

class NodeHardware(object):
    def __init__(self):
        pass
    
    def available_cpus( self ):
        return multiprocessing.cpu_count()

    
