import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import time

if __name__ == "__main__":
    # Local pub and sub
    lpub = nn.Socket( nn.PUB, domain=nn.AF_SP )
    lpub.connect( "tcp://0.0.0.0:8686" )
    lpub.send( "test" )

    while( True ):
        lpub.send( "test" )
        time.sleep( 0.5 )

