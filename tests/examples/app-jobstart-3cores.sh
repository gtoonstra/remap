#!/bin/bash

# Create the directory for output by reduce (this is always overwritten)
mkdir REMAP_ROOT/data/wordcount/

# Clean potential data from previous run, because that is not overwritten;
# (mapper-id is always different)
rm -rf REMAP_ROOT/job/3cores/part/*

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.3cores {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"mapper","inputfile":"REMAP_ROOT/data/tomsawyer.txt","outputdir":"REMAP_ROOT/job/3cores/part"},{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"mapper","inputfile":"REMAP_ROOT/data/beowulf.txt","outputdir":"REMAP_ROOT/job/3cores/part"},{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"mapper","inputfile":"REMAP_ROOT/data/alice-in-wonderland.txt","outputdir":"REMAP_ROOT/job/3cores/part"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.3cores {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/3cores/part/a2e","partition":"a2e"},{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/3cores/part/_default","partition":"_default"},{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/3cores/part/f2n","partition":"f2n"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.3cores {"priority":5,"appdir":"REMAP_ROOT/app/wordcount","cores":[{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/3cores/part/o2s","partition":"o2s"},{"jobid":"3cores","appmodule":"wordcount","appconfig":"REMAP_ROOT/app/wordcount/appconfig.json","type":"reducer","outputdir":"REMAP_ROOT/data/wordcount","inputdir":"REMAP_ROOT/job/3cores/part/t2z","partition":"t2z"}]}'
