#!/bin/bash

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"/remote/job/jobid/app/appconfig.json","type":"mapper","inputfile":"/remote/data/tomsawyer.txt","outputdir":"/remote/job/jobid/part"}]}'

sleep 5



