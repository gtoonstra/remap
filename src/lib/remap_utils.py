# library utility functions
import json
import uuid
import logging

logger = logging.getLogger(__name__)

class RemapException(Exception):
    pass

def unpack_msg( msg ):
    try:
        msgtype, data = msg.decode("utf-8").split(" ", 1 )
        try:
            data = json.loads( data )
            return msgtype, data
        except ValueError as ve:
            raise RemapException( "Invalid json data payload" ) from ve
    except ValueError as ve:
        raise RemapException( "Invalid message" ) from ve

def pack_msg( prefix, data ):
    msg = "%s %s"%( prefix, json.dumps( data ))
    return msg

def unique_id():
    return str(uuid.uuid1())

def node_id():
    return str(uuid.getnode())

def core_id( nodeid, pid ):
    return "%s_%d"%( nodeid, pid )

def extract_node_id( coreid ):
    return coreid.split("_")[0]

def safe_get( data, key ):
    if key not in data:
        raise RemapException("Required key %s not found in data."%( key ))
    return data[ key ]

def split_prefix( prefix ):
    try:
        r,t,s = prefix.split(".")
        return r,t,s
    except ValueError as ve:
        raise RemapException( "Invalid prefix %s"%( prefix ) ) from ve

