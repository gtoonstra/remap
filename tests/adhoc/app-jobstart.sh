#!/bin/bash

nanocat --pub --connect-local 8686 --delay 1 --data 'local.jobstart.jobid {"priority":5,"cores":[{"appdir":"/remote/app/wordcount","appmodule":"wordcount","appconfig":"/remote/app/wordcount/appconfig.json","type":"mapper","input":"/remote/data/tomsawyer.txt","output":"/remote/im/jobid"}]}'

