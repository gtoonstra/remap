import remap

# --- create file i/o objects to be used ----
def create_vertex_reader( filename ):
    return remap.TextFileReader( filename, yieldkv=False )

def create_vertex_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid )

NUM_VERTICES = 500

# ---- pagerank vertex implementation ----
def prepare( line ):
    if len(line) == 0:
        return None, None

    elems = line.strip().split(" ")
    tuples = []
    for i in range(1,len(elems)):
        if len(elems[i]) > 0:
            tuples.append( elems[ i ] )

    vertex = ( 1.0 / NUM_VERTICES, tuples )
    return elems[0], vertex

def compute( send_fn, superstep, vertex, messages ):
    (val, tuples) = vertex
    if (superstep >= 1):
        sum = 0
        
        print( vertex, messages )
        for data in messages:
            sum = sum + float(data)

        val = 0.15 / NUM_VERTICES + 0.85 * sum
        vertex = ( val, tuples )

    if superstep < 30:
        for vertex_id in tuples:
            send_fn( vertex_id, "%f"%( val / len(tuples) ))
    else:
        return vertex, True

    return vertex, False

