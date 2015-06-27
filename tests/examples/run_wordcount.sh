#!/bin/bash

# Create the directory for output by reduce (this is always overwritten)
mkdir REMAP_ROOT/data/wordcount/

# Clean potential data from previous run, because that is not overwritten;
# (mapper-id is always different)
rm -rf REMAP_ROOT/job/jobid/part/*

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"mapper","inputfile":"REMAP_ROOT/data/gutenberg/tomsawyer.txt","outputdir":"REMAP_ROOT/job/jobid/part"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/jobid/part/a2e","partition":"a2e"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/jobid/part/_default","partition":"_default"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/jobid/part/f2n","partition":"f2n"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/jobid/part/o2s","partition":"o2s"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/jobid/part/t2z","partition":"t2z"}]}'

