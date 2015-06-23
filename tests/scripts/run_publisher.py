import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import time

if __name__ == "__main__":
    # Local pub and sub
    lpub = nn.Socket( nn.PUB, domain=nn.AF_SP )
    lpub.connect( "tcp://localhost:8686" )
    lpub.send( "test" )

    while( True ):
        lpub.send( "global.test.09325235325_12124 {}" )
        time.sleep( 5 )

