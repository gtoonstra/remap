import select
import sys
import pybonjour
import threading
import logging

logger = logging.getLogger(__name__)

class BonjourRegistration(object):
    def __init__( self, name, regtype, port ):
        self.name = name
        self.regtype = regtype
        self.port = port
        self.t = threading.Thread(target=self.run, args=())
        self.t.daemon = True

    def start( self ):
        self.t.start()

    def run( self ):
        logger.info( "Registering broker as %s under %s"%( self.name, self.regtype ))
        sdRef = pybonjour.DNSServiceRegister(name = self.name,
            regtype = self.regtype,
            port = self.port,
            callBack = self.register_callback)

        try:
            try:
                while True:
                    ready = select.select([sdRef], [], [])
                    if sdRef in ready[0]:
                        pybonjour.DNSServiceProcessResult(sdRef)
            except KeyboardInterrupt:
                pass
        finally:
            sdRef.close()

    def register_callback(self, sdRef, flags, errorCode, name, regtype, domain):
        if errorCode == pybonjour.kDNSServiceErr_NoError:
            logger.info( "Registered broker as bonjour service" )
        else:
            logger.info( "Registration of broker over bonjour failed." )

