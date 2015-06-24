import remap

# --- create file i/o objects to be used ----
def create_mapper_reader( filename ):
    return remap.TextFileReader( filename )

def create_mapper_partitioner( outputdir, partition, mapperid ):
    return remap.TextPartitioner( outputdir, partition, mapperid )

def create_reducer_reader( inputdir ):
    return remap.TextPartFileReader( inputdir )

def create_reducer_writer( outputdir, partition ):
    return remap.TextReduceWriter( outputdir, partition )

# ---- map and reduce implementations ----

# map just creates one record of the word and a '1' to count it,
# it also directs the mapped value to a specific partition
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

# The reduce operation sums all the values in the sequence and outputs.
def reduce( key, list_of_values ):
    yield (key, sum(list_of_values))

