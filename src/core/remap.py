import os 
import errno
import json

class BaseReader(object):
    def __init__(self,filename):
        self.filename = filename
        self.filesize = os.stat(filename).st_size 
        self.complete = False

    def isComplete( self ):
        return self.complete

# A class for reading in raw data to be processed.
# Used as input to the mapper
class TextFileReader(BaseReader):
    def __init__( self, filename ):
        BaseReader.__init__(self,filename)
        self.f = open(self.filename, 'r')
        self.pos = 0

    def read( self ):
        for line in self.f:
            self.pos = self.pos + len(line)
            yield self.filename, line
        self.complete = True

    def progress( self ):
        return float( float(self.pos) / self.filesize ) * 100

    def close( self ):
        self.f.close()

# A partitioner creates intermediate data. It is responsible for accepting large volumes of
# key,value data. If the output file need not be sorted, it can write this to file directly.
# If sorting is necessary, it should keep things in memory, write it to disk when memory is full
# and create a new partition file for the same mapper
class BasePartitioner( object ):
    def __init__( self, outputdir, partition, mapperid ):
        self.outputdir = os.path.join( outputdir, partition )
        self.partition = partition
        self.mapperid = mapperid
        self.mem = {}
        self.total_keys = 0
        self.total_values = 0
        self.sequence = 0
        self.filename = os.path.join( self.outputdir, "part-%s-%05d"%( self.mapperid, self.sequence ) )

        try:
            os.makedirs( os.path.dirname( self.filename ) )
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST:
                pass
            else: 
                raise

    # Statistics handling here allow future splitting up of further data
    # if this partition overfloweth.
    def store( self, k2, v2  ):
        if k2 not in self.mem:
            self.mem[ k2 ] = []
            self.total_keys = self.total_keys + 1

        self.mem[ k2 ].append( v2 )
        self.total_values = self.total_values + 1

class TextPartitioner( BasePartitioner ):
    def __init__( self, outputdir, partition, mapperid ):
        BasePartitioner.__init__(self, outputdir, partition, mapperid )
        self.f = open(self.filename, 'w')

    def sort_flush_close( self ):
        for k in sorted(self.mem):
            l = self.mem[k]
            out = json.dumps( l )
            self.f.write( "%s,%s\n"%( k,out ) )
        self.f.close()

# The part file reader reads back in one single partition file.
class TextPartFileReader(BaseReader):
    def __init__( self, filename ):
        BaseReader.__init__(self,filename)
        self.f = open(filename, 'r')
        self.pos = 0

    def read( self ):
        for line in self.f:
            key, data = line.split(',', 1)
            l = json.loads( data )
            yield (key, l, len(line))
        self.complete = True

    def isComplete( self ):
        return self.complete

    def progress( self ):
        return float( float(self.pos) / self.filesize )

    def close( self ):
        self.f.close()

# The reduce writer writes out the final result
class BaseReduceWriter( object ):
    def __init__(self, partdir, partition):
        self.partdir = partdir
        self.partition = partition

class TextReduceWriter( BaseReduceWriter ):
    def __init__( self, partdir, partition ):
        BaseReduceWriter.__init__( self, partdir, partition )
        self.filename = os.path.join( self.partdir, "reduce_%s"%( partition ))
        self.f = open(self.filename, 'w')

    def store( self, k3, v3  ):
        self.f.write( "%s,%d\n"%( k3, v3 ) )

    def close( self ):
        self.f.close()

