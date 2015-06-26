import select
import sys
import pybonjour
import logging
import pybonjour
import threading

logger = logging.getLogger(__name__)

class BonjourResolver( object ):
    def __init__( self, regtype, callback ):
        self.t = threading.Thread(target=self.run, args=())
        self.t.daemon = True
        self.regtype = regtype
        self.timeout = 5
        self.callback = callback

    def start( self ):
        self.resolved = []
        self.t.start()

    def resolve_callback(self, sdRef, flags, interfaceIndex, errorCode, fullname,
                         hosttarget, port, txtRecord):
        if errorCode == pybonjour.kDNSServiceErr_NoError:
            logger.info( 'Resolved service %s at %s'%( fullname, hosttarget ))
            self.resolved.append(True)
            self.hosttarget = hosttarget
            self.port = port
            hosttarget = hosttarget.rstrip('.')
            self.callback( hosttarget )
        else:
            logger.info( errorCode )
            return

    def browse_callback(self,sdRef, flags, interfaceIndex, errorCode, serviceName,
                        regtype, replyDomain):
        if errorCode != pybonjour.kDNSServiceErr_NoError:
            return

        if not (flags & pybonjour.kDNSServiceFlagsAdd):
            logger.info( 'The service entry was removed' )
            self.callback( "unknown" )
            return

        logger.info( 'Another service identified, resolving' )

        resolve_sdRef = pybonjour.DNSServiceResolve(0,
                                                    interfaceIndex,
                                                    serviceName,
                                                    regtype,
                                                    replyDomain,
                                                    self.resolve_callback)

        try:
            while not self.resolved:
                ready = select.select([resolve_sdRef], [], [], self.timeout)
                if resolve_sdRef not in ready[0]:
                    logger.info( 'Resolution timed out' )
                    break
                pybonjour.DNSServiceProcessResult(resolve_sdRef)
            else:
                self.resolved.pop()
        finally:
            resolve_sdRef.close()

    def run( self ):    
        browse_sdRef = pybonjour.DNSServiceBrowse(regtype = self.regtype,
                                                  callBack = self.browse_callback)

        try:
            try:
                while True:
                    ready = select.select([browse_sdRef], [], [])
                    if browse_sdRef in ready[0]:
                        pybonjour.DNSServiceProcessResult(browse_sdRef)
            except KeyboardInterrupt:
                pass
        finally:
            browse_sdRef.close()

