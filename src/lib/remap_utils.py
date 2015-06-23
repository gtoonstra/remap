# library utility functions
import json
import uuid

class RemapException(Exception):
    pass

def unpack_msg( msg ):
    msgtype, data = msg.decode("utf-8").split(" ", 1 )
    data = json.loads( data )
    return msgtype, data

def pack_msg( prefix, data ):
    msg = "%s %s"%( prefix, json.dumps( data ))
    return msg

def unique_id():
    return str(uuid.uuid1())

def node_id():
    return str(uuid.getnode())

def core_id( nodeid, pid ):
    return "%s_%d"%( nodeid, pid )

def safe_get( data, key ):
    if key not in data:
        raise RemapException("Required key %s not found in data."%( key ))
    return data[ key ]

