import remap

# --- create file i/o objects to be used ----
def create_vertex_reader( filename ):
    return remap.TextFileReader( filename, yieldkv=False )

def create_vertex_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid )

# ---- pagerank vertex implementation ----
def prepare( line ):
    line = line.strip()
    if len(line) == 0:
        return None, None

    elems = line.split()
    out = []

    for i in range(2,len(elems)):
        if len(elems[i]) > 0:
            out.append( elems[ i ] )

    vertex = ( int(elems[1]), out )
    return elems[0], vertex

def compute( forward, sub, unsub, superstep, vertex, messages ):
    (val, out) = vertex

    halt = True
    for data in messages:
        if int(data) > val:
            val = int(data)
            halt = False

    if superstep == 0:
        halt = False

    vertex = (val,out)

    for vertex_id in out:
        forward( vertex_id, "%d"%( val ))

    return vertex, halt


