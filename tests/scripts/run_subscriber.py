import sys
import os
import nanomsg as nn
from nanomsg import wrapper as nn_wrapper
import time

if __name__ == "__main__":
    # Local pub and sub
    lsub = nn.Socket( nn.SUB, domain=nn.AF_SP )
    lsub.connect( "tcp://localhost:8687" )
    lsub.set_string_option( nn.SUB, nn.SUB_SUBSCRIBE, "")
    data = lsub.recv()
    print(data)

