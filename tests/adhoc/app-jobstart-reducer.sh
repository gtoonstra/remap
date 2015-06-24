#!/bin/bash

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"/remote/job/jobid/app/appconfig.json","type":"reducer","outputdir":"/remote/data/wordscounted","inputdir":"/remote/job/jobid/part/a2e","partition":"a2e"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"/remote/job/jobid/app/appconfig.json","type":"reducer","outputdir":"/remote/data/wordscounted","inputdir":"/remote/job/jobid/part/_default","partition":"_default"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"/remote/job/jobid/app/appconfig.json","type":"reducer","outputdir":"/remote/data/wordscounted","inputdir":"/remote/job/jobid/part/f2n","partition":"f2n"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"/remote/job/jobid/app/appconfig.json","type":"reducer","outputdir":"/remote/data/wordscounted","inputdir":"/remote/job/jobid/part/o2s","partition":"o2s"}]}'

sleep 5

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"appdir":"/remote/job/jobid/app","cores":[{"jobid":"jobid","appmodule":"wordcount","appconfig":"/remote/job/jobid/app/appconfig.json","type":"reducer","outputdir":"/remote/data/wordscounted","inputdir":"/remote/job/jobid/part/t2z","partition":"t2z"}]}'


