# mreasy

Mr. Easy is an implementation of MapReduce in pure python

The focus of this implementation is on:
- Very low complexity and number of steps for installation
- From the perspective of the app developer, no difference if run locally or in a thousands node cluster
- Sensible, minimalistic "expert" API, no specific API for I/O or k,v pair handling
- Support for multi-splitting of data (one mapper with multiple reducer 'sets')
- Uses existing "infrastructure" solutions to solve hard stuff (distributed fs, etc)
- Pristine input/output files. I.e., you can run this on and against anything
- Targeted for modest 3-10 node clusters

## Design

***File System***

MR requires a file system where nodes can read and write files. 
CephFS is suggested as such a file system, because the use of cephfs can be transparent to the 
end user. This simplifies the setup, because it makes no difference if it's installed or not.

This does require a bit of thinking where files appear and go. There are two different trees that
can be imagined that dictate how files are written by the application; some files are local logfiles
and should be kept on the local disk and not be distributed. Other files are config files or app files
and should be kept on the entire network. Other files are data files that should be distributed
as well.

***Processing requests***

The implementation is heavily decentralized. From the outside environment, an app initiator
may send a "processing request" into the network, specifying the number of cores that need to be part
of the processing run. This number is based on the size of the network and the processing priority.
Low priority tasks simply get less cores, high priority tasks get more cores. This means that mapper and
reduce tasks are run in serial rather than parallel.

A core is a daemon on a machine that can start/stop any mapper or reducer process. The core is connected
to the network through a messaging bus architecture. A core runs on a node. The number of cores per node
is dependent on the performance of the CPU and the hard drive. The node runs a daemon that controls the 
core processes. It is a watchdog server and deals out "utility rates". The utility rate depends on the 
current CPU and HD usage and cores request the utility rate from the node daemon. If a utility rate was
just passed in, then the following utility rate request is reduced with a certain number, depending on
the expected reduction of utility if the first request is granted (optimistic planning scheme). This means that
a node with 32 idle cores won't get 32 processes to execute.

The processing request flow is an iterative process; it lists the expected utility that 
a core should have to participate and nodes respond when their current utility is better than the expected
utility. The app initiator receives the set of responders and deals out roles to the cores for 
the mapping and reduction stages. 

If the number of cores has not yet been reached, the initiator reduces the expected utility rate by 10 and
runs again. Cores that have not yet answered and which now classify respond to the request at this time,
growing the set of cores for the app initiator until the number of cores has been reached. This is iteratively
done until the number is reached or 0 utility is reached. In the latter case the MR process starts with the
number of cores that are present.

If there are no cores to initiate the app, the initiator prints an error.

If a node has 4 cores, then 3 cores can be instantiated to do actual work. 1 cpu cure is utilized for os work 
and the core daemon communicating with the network and the comms with the core child processes. 

The output directory where processed files are to be located may not already exist.

If the app initiator dies, the cores stop processing after 5 * heartbeats without confirmation.

If the app initiator is terminated (ctrl-C), the initiator sends out a spawned request to kill cores that
may be processing.

***The mapper***

One mapper is started for each file to be processed. The core daemon has received the following information:
- the in file to be processed
- the directories to produce output files
- the number of reducers per collection.

The core instantiates the file reader, passing the name of the input file. The input file reader is then
ready to produce k,v pairs. As soon as the core is ready, it calls "process" to process the next 100 records.

The core also instantiates formatter plugin objects for each reducer set and number of reducers, so one formatter
plugin object per output file. 

The mapper reader plugin actually reads the file. No attempt is made by the core to read or understand the file format. 
This reader 'yields' the results for each object in a key/value set to the core. The core passes the k,v set to the 
mapper plugin along with a dictionary of formatters. The mapper plugin maps the object into new kx,vx objects and
calls its specific formatter for each type, as many times as needed ( or 0 ).

The core creates a new instance of the formatter for each output file. So the formatter must implement "init" to 
initialize the output file. The core passes in the filename. After every 50 mapping operations and if a new operation is about
to take place, the core verifies file size on disk and creates a new instance of the formatter plugin if the file size is
exceeded, telling the old formatter to close the file prior to being disposed.

The output of the mapper can be in any file format imaginable. The implementation for working with those file types is
at the discretion of the mapper process, but obviously the reducer must be able to work with that filetype as well
( to read it ). 

***The reducer***

The reducer has up to as many input files as there are mappers. At this point it is guaranteed that one reducer will process
the input files and that there are no keys in the data that should belong to a different reducer.

Prior to starting the reducer, the core sorts the input files. 

In this case, the same core process starts a workflow to deal with a reducer instead. It uses the same process to create
an input file reader object, passing in the filename to open. It is single-threaded, so the core opens one input file 
at a time. 

The reducer produces output files in the final output directory. It overwrites existing files.

***Core daemon responsibilities***
The core is responsible for maintaining the validity of the output files. A mapper process either succeeds when everything is
confirmed processed in its entirety or it fails in its entirety. 

The core daemon should produce 1 minute heartbeats to the app initiator to indicate it is still processing.
This heartbeat message contains a percentage of completeness.

It is not necessary to include any output files, because the output file names can always be derived by other means.

***Directory trees***

<jobid> = a uuid to identify a specific job run.
<coreid> = locally unique identifier for a core, unique per node.

/remote = mount for distributed file system (ceph). Or... just a created directory on a local system, or a softlink on a local system
/local = mount for local file system. Always a directory on a local system, or a softlink to some other location
/remote/job/<jobid> = main directory where a job is initiated
/remote/job/<jobid>/app = App files are copied here, so that it is known which version of the app was used for the job
/remote/app/xxx/yyy/zzz = directory to files for a particular application
/remote/data/xxx/yyy/zzz = data directory with custom hierarchy, user specific, input or output
/remote/im/<jobid>/reducer-0000 = Intermediate files destined for reducer-0000
/remote/im/<jobid>/reducer-0001 = Intermediate files destined for reducer-0001, etc...
/remote/cluster/blacklist = A file with blacklisted nodes.
/local/tmp/<coreid>/whatever = Temporary local files for mappers or reducers, if needed

***Files of interest***

/remote/app/xxx/yyy/zzz/app.json = Configuration file for the app.

/remote/job/<jobid>/app.log = Global log file that indicates general progress for the run. Written to by the app initiator only.
/remote/job/<jobid>/jobconfig.json = File indicating how the network resolved nodes/cores to process the application. Explains the config of the job.
/remote/job/<jobid>/progress/mapper-0000.json = Progress file for mapper 0000, written in 10% quartiles.
/remote/job/<jobid>/progress/mapper-0001.json = Progress file for mapper 0001, written in 10% quartiles.
/remote/job/<jobid>/progress/reducer-0000.json = Progress file for reducer 0000, written in 10% quartiles.
/remote/job/<jobid>/consolidated-progress.json = Consolidated progress file.
/remote/job/<jobid>/result = Result file for the job: INCOMPLETE, FAIL, OK

***Starting a job***

> ./app_initiator com.hooli.mrwordcount /remote/data/gutenberg/txt/ /remote/data/gutenberg/wordcounts/

The initiator must keep running on the console by default. You can add the "-ignore_me" flag to allow the 
cluster to continue processing without the initiator. 

The job can be stopped by terminating the initiator or by issuing a global 'kill <jobid>' command to which
all cores should respond.

***Mapper/reducer file templates***

mapper input file: 
can be anything (folder indicated by user). <folder>/file1.txt  (etc)

mapper output file:
<purpose> = a string identifying the purpose for the reducer. 
<reducer-id> = the id of a specific reducer
<mapper-id> = this mapper id
<part-id> = files created at boundaries of 64MB filesize. 

/remote/im/<jobid>/<purpose>/<reducer-id>/mapper-<mapperid>-part-<partid>
/remote/im/<jobid>/default/reducer-0000/mapper-0000-part-00000
/remote/im/<jobid>/default/reducer-0001/mapper-0000-part-00000
/remote/im/<jobid>/default/reducer-0001/mapper-0000-part-00001
/remote/im/<jobid>/default/reducer-0002/mapper-0000-part-00000
/remote/im/<jobid>/default/reducer-0000/mapper-0000-part-00001
/remote/im/<jobid>/byage/reducer-0001/mapper-0000-part-00000
/remote/im/<jobid>/byage/reducer-0002/mapper-0000-part-00000

reducer input file(s):
/remote/im/<jobid>/<reducer-id>/*
/remote/im/<jobid>/default/reducer-0001/mapper-0000-part-00000
/remote/im/<jobid>/default/reducer-0001/mapper-0000-part-00001
/remote/im/<jobid>/default/reducer-0001/mapper-0001-part-00000
/remote/im/<jobid>/default/reducer-0001/mapper-0003-part-00003

reducer output file(s):
/remote/data/<hierarchy>/wordcounts/default/reducer-0000-part-00000
/remote/data/<hierarchy>/wordcounts/default/reducer-0000-part-00001
/remote/data/<hierarchy>/wordcounts/default/reducer-0001-part-00000
/remote/data/<hierarchy>/wordcounts/byage/reducer-0001-part-00000

