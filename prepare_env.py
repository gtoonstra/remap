#!/usr/bin/python3.4

import os
import sys

print("(The root directory for remap files must be writable for the current user)")
root = raw_input("Where to create the directory layout for remap? : ")

confirm = raw_input("Do you really want to install remap at %s?  (y/n)  : "%( root ))
if confirm != "y":
    print("Install cancelled.")
    sys.exit(-1)

# /remote/app/xxx/yyy/zzz = directory to files for a particular application

def create_dirs( newdir ):
    try:
        os.makedirs( newdir )
    except OSError as oe:
        pass

create_dirs( os.path.join( root, "job" ) )
create_dirs( os.path.join( root, "app" ) )
create_dirs( os.path.join( root, "data" ) )
create_dirs( os.path.join( root, "cluster" ) )


