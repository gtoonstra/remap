import remap

# --- file i/o objects ----
def create_mapper_reader( filename ):
    return remap.TextFileReader( filename )

def create_mapper_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid )

def create_reducer_reader( inputdir ):
    return remap.TextPartFileReader( inputdir )

def create_reducer_writer( outputdir, partition ):
    return remap.TextReduceWriter( outputdir, partition )

# ---- map and reduce implementations ----
# map creates a <word>,1 combination and also 
# allocates them to specific partitions, depending on the number
# of reducers
def map( key, value ):
    remove = ".,?:;!\""
    trans = str.maketrans(remove, ' ' * len(remove))

    words = value.translate( trans ).split()
    for word in words:
        # remove comma's, they create issues for our file format
        word = word.lower()
        if word[0] in 'abcde':
            yield 'a2e', word, 1
        elif word[0] in 'fghijklmn':
            yield 'f2n', word, 1
        elif word[0] in 'opqrs':
            yield 'o2s', word, 1
        elif word[0] in 'tuvwxyz':
            yield 't2z', word, 1
        else:
            yield '_default', word, 1

def reduce( key, list_of_values ):
    yield (key, sum(list_of_values))

