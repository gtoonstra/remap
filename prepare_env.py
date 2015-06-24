#!/usr/bin/python3.4

import os
import sys
from distutils import dir_util
import fileinput

print("(The root directory for remap files must be writable for the current user)")
root = input("Where to create the directory layout for remap? : ")

confirm = input("Do you really want to install remap at %s?  (y/n)  : "%( root ))
if confirm != "y":
    print("Install cancelled.")
    sys.exit(-1)

# /remote/app/xxx/yyy/zzz = directory to files for a particular application

def create_dirs( newdir ):
    try:
        os.makedirs( newdir )
    except OSError as oe:
        pass

# create directory structure
create_dirs( os.path.join( root, "job" ) )
create_dirs( os.path.join( root, "app" ) )
create_dirs( os.path.join( root, "data" ) )
create_dirs( os.path.join( root, "cluster" ) )
create_dirs( os.path.join( root, "test" ) )

# Get location this file is at (remap git directory)
modpath = os.path.realpath(os.path.dirname(os.path.abspath(__file__)))

# Copy data from remap git to "data" directory in new location
srctree = os.path.join( modpath, "testdata/" )
dir_util.copy_tree( srctree, os.path.join( root, "data" ) )

# Copy examples from remap git to "app" directory in new location
srctree = os.path.join( modpath, "examples/" )
dir_util.copy_tree( srctree, os.path.join( root, "app" ) )

# Copy test scripts from remap git to "test" directory in new location
srctree = os.path.join( modpath, "tests/examples/" )
dir_util.copy_tree( srctree, os.path.join( root, "test" ) )

all_test_scripts = os.listdir( os.path.join( root, "test" ) )

for filename in all_test_scripts:
    fullpath = os.path.join( root, "test", filename )
    for line in fileinput.input([fullpath], inplace=True):
        print(line.replace('REMAP_ROOT', root ))

    os.chmod(fullpath, 0o755)

