# library utility functions
import json
import uuid

def unpack_msg( msg ):
    msgtype, data = msg.decode("utf-8").split(" ", 1 )
    data = json.loads( data )
    return msgtype, data

def pack_msg( prefix, data ):
    msg = "%s %s"%( prefix, json.dumps( data ))
    return msg

def unique_id():
    return str(uuid.uuid1())

