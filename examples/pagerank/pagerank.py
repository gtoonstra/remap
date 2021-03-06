import remap

# --- create file i/o objects to be used ----
def create_vertex_reader( filename ):
    return remap.TextFileReader( filename, yieldkv=False )

def create_vertex_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid )

NUM_VERTICES = 10

# ---- pagerank vertex implementation ----
def prepare( line ):
    line = line.strip()
    if len(line) == 0:
        return None, None

    elems = line.split()

    out = []
    for i in range(1,len(elems)):
        if len(elems[i]) > 0:
            out.append( elems[ i ] )

    vertex = ( 1.0 / NUM_VERTICES, out )
    return elems[0], vertex

def compute( send_fn, superstep, vertex, messages ):
    (val, out) = vertex
    if (superstep >= 1):
        sum = 0
        
        for data in messages:
            sum = sum + float(data)

        val = 0.15 / NUM_VERTICES + 0.85 * sum
        vertex = ( val, out )

    if superstep < 30:
        for vertex_id in out:
            send_fn( vertex_id, "%f"%( val / len(out) ))
    else:
        return vertex, True

    return vertex, False

