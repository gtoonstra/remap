import remap

# --- create file i/o objects to be used ----
def create_mapper_reader( filename ):
    return remap.HTMLFileReader( filename )

def create_mapper_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid, combiner=list_combiner )

def create_reducer_reader( inputdir ):
    return remap.TextPartFileReader( inputdir )

def create_reducer_writer( outputdir, partition ):
    return remap.TextReduceWriter( outputdir, partition )

# ---- map and reduce implementations ----

def list_combiner( l ):
    return list(set(l))

def map( key, value ):
    remove = ".,?:;!\""
    trans = str.maketrans(remove, ' ' * len(remove))

    words = value.translate( trans ).split()
    for word in words:
        # remove comma's, they create issues for our file format
        word = word.lower()
        if word[0] in 'abcde':
            yield 'a2e', word, key
        elif word[0] in 'fghijklmn':
            yield 'f2n', word, key
        elif word[0] in 'opqrs':
            yield 'o2s', word, key
        elif word[0] in 'tuvwxyz':
            yield 't2z', word, key
        else:
            yield '_default', word, key

# The reduce operation sums all the values in the sequence and outputs.
def reduce( key, list_of_values ):
    yield (key, list_combiner(list_of_values))

