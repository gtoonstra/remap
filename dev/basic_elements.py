import os
import json
import errno

# A class for reading in raw data to be processed.
# Used as input to the mapper
class TextFileReader(object):
    def __init__( self, filename ):
        # super(object, self).__init__()
        self.f = open(filename, 'r')
        self.filename = filename

    def read( self ):
        for line in self.f:
            yield self.filename, line

    def close( self ):
        self.f.close()

# A class that maintains intermediate data. The data is kept in memory,
# so that chunks can be written out to file in a controlled manner,
# such that at least each partition subfile is fully sorted.
class Partitioner( object ):
    def __init__( self, filename ):
        # super(object, self).__init__()
        try:
            os.makedirs( os.path.dirname( filename ) )
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST:
                pass
            else: raise
        self.f = open(filename, 'w')
        self.mem = {}
        self.total_keys = 0
        self.total_values = 0

    # Statistics handling here allow future splitting up of further data
    # if this partition overfloweth.
    def store( self, k2, v2  ):
        if k2 not in self.mem:
            self.mem[ k2 ] = []
            self.total_keys = self.total_keys + 1

        self.mem[ k2 ].append( v2 )
        self.total_values = self.total_values + 1

    def sort_flush_close( self ):
        for k in sorted(self.mem):
            l = self.mem[k]
            out = json.dumps( l )
            self.f.write( "%s,%s\n"%( k,out ) )
        self.f.close()

# The part file reader reads back in one single partition file.
class PartFileReader(object):
    def __init__( self, filename ):
        # super(object, self).__init__()
        self.f = open(filename, 'r')

    def read( self ):
        for line in self.f:
            key, data = line.split(',', 1)
            l = json.loads( data )
            yield (key, l)

# The reduce writer dumps the final results to some file for one single
# reducer instance. 
class ReduceWriter( object ):
    def __init__( self, filename ):
        # super(object, self).__init__()
        self.f = open(filename, 'w')

    def store( self, k3, v3  ):
        self.f.write( "%s,%d\n"%( k3, v3 ) )

    def close( self ):
        self.f.close()

# ---- map and reduce implementations ----
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


# --- main program start ---
if __name__ == '__main__':

    # ---- what a mapper does ----

    # read in data
    # A 'real life' example would have many input files and thus multiple instances
    # of this 'mapper' object, distributed across nodes.
    fr = TextFileReader( "../testdata/tomsawyer.txt" )

    # set up partitions for the mapper output data.
    # every mapper instance does this.
    partitions = {}
    partitions[ "a2e" ] = Partitioner( "../im/a2e/part-r-0000.txt" )
    partitions[ "f2n" ] = Partitioner( "../im/f2n/part-r-0000.txt" )
    partitions[ "o2s" ] = Partitioner( "../im/o2s/part-r-0000.txt" )
    partitions[ "t2z" ] = Partitioner( "../im/t2z/part-r-0000.txt" )
    partitions[ "_default" ] = Partitioner( "../im/default/part-r-0000.txt" )

    # Map it.
    for k1, v1 in fr.read():
        for part, k2, v2 in map( k1, v1 ):
            partitions[ part ].store( k2, v2 )

    fr.close()
    fr = None

    # Sort partition before output and flush sorted output to file.
    for part in partitions:
        partitions[part].sort_flush_close()

    # ---- what a reducer does ----

    # Grab the partitions for this reducer instance, made by some anonymous
    # collection of mappers.
    # In real life, you'd have one reducer per partition key. So here we simply reuse
    # the reducer in a serial fashion. Starting with partition #1, run reducer, partition #2,
    # run reducer, etc...
    #
    # The partition is not necessarily a single file, it can be many if the data overflows
    # the max file size (or memory).
    partitions = {}
    partitions[ "a2e" ] = PartFileReader( "../im/a2e/part-r-0000.txt" )
    partitions[ "f2n" ] = PartFileReader( "../im/f2n/part-r-0000.txt" )
    partitions[ "o2s" ] = PartFileReader( "../im/o2s/part-r-0000.txt" )
    partitions[ "t2z" ] = PartFileReader( "../im/t2z/part-r-0000.txt" )
    partitions[ "_default" ] = PartFileReader( "../im/default/part-r-0000.txt" )

    # A way to output the results of the reduce operation.
    rw = ReduceWriter( "../im/result-r-0000.txt" )

    # reduce data and produce results.
    # In real life, the reducewriter would be recreated for each partition, which is another way
    # of saying that reducers are actually isolated from each other and have their own output files.
    for part in sorted(partitions):
        for key,list_of_values in partitions[part].read():
            for k3,v3 in reduce( key, list_of_values ):
                rw.store( k3, v3 )

    rw.close()

