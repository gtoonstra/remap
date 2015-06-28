import remap
from operator import itemgetter

# --- create file i/o objects to be used ----
def create_mapper_reader( filename ):
    return remap.TextFileReader( filename )

def create_mapper_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid, combiner=None, customkey=itemgetter(3) )

...etc...

# ---- map and reduce implementations ----
def map( key, value ):
    words = value.split(',')
    part = words[2].replace(" ", "_")
    yield part, tuple(words), ""

def reduce( key, value ):
    yield (key, value)

