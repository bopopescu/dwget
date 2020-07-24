# coding=utf8
from common.consts import *
from common.Logger import Logger
from threads import  *
from report import Report

from pydispatch import dispatcher

from xml.etree import ElementTree as ET
from xml.etree.ElementTree import *

import sys

class subordinateManager():
    """
    Class responsible for all kind of communication with a subordinate. It handles the queue
    which stores all of the requests send to a subordinate. It also stores handle to the file
    with the file fragment downloaded.
    """
    __taskQueue=deque()
    __tempFile = None
    __allocatedBytes = 0
    dlThread = None
    __logger = None
    state = JUST_STARTED
    
    def __init__(self):
        dispatcher.connect(self.__requestReceived, signal = 'MASTER_REQUEST', sender = dispatcher.Any)
        dispatcher.connect(self.__changeState, signal = 'STATE_CHANGE', sender = dispatcher.Any)
        self.__tempFile = NamedTemporaryFile(suffix = '.dwgetds')
        #self.__logger = Logger("dwgetds")
        
    def __allocateSpace(self, toAllocate):
        """
        Guarantees toAllocate bytes available in the temp file.
        If the request cannot be completed - raises exception. 
        """ 
        toAllocate -= self.__allocatedBytes       # How much more space do we need?
        dispatcher.send('DEBUG', 'subordinateManager.__tempFile', 'Allocating %d bytes more.' % (toAllocate)) 
        fill = ''
        for i in xrange(65536):                 # Prepare the fill (64 kB.
            fill = fill + 'x' 
        self.__tempFile.seek(self.__allocatedBytes) # Don't allocate from the beginning, just how much more we need.
        while toAllocate > 0:
            if toAllocate > 65536:              # Writes chunks of 64 kB for performance.
                toAllocate -= 65536
                self.__tempFile.write(fill)
            else:
                self.__tempFile.write(fill[:toAllocate])
                break
            
    def __changeState(self, arg, signal, sender): 
        """
        Implements the changeState signal handler.
        """ 
        dispatcher.send('DEBUG', 'subordinateManager.subscriber', 'Changed state to %d.' %(arg[0])) 
        self.state = arg[0]
    def __requestReceived(self, arg, signal, sender): 
        """
        Implements the requestReceived signal handler.
        """ 
        dispatcher.send('DEBUG', 'subordinateManager.subscriber', 'Received new request %d.' %(arg[0])) 
        self.__taskQueue.append([arg, False]) # arg -> task, false -> not being done now.
        self.__doTask()
        
    def __doTask(self):
        if len(self.__taskQueue) == 0:
            return
        
        if not self.__taskQueue[0][1]:    # Is task in front of the queue not being done yet?
            dispatcher.send('DEBUG', 'subordinateManager.taskQueue', 'Dealing with new request.') 
            self.__taskQueue[0][1] = True # If so, set it as being done.
            
            if self.__taskQueue[0][0][0] == NEW_URI:  # 'Start new download' task.
                dispatcher.send('DEBUG', 'subordinateManager.taskQueue', 'NEW_URI request.') 
                (url, begin, length) = self.__taskQueue[0][0][1:] 
                # cancel current DL
                if self.dlThread is not None:
                    self.dlThread.cancel()
                    self.dlThread.join()             # wait until cancelled properly
                
                # allocate tmp
                if length > self.__allocatedBytes:
                    try:
                        dispatcher.send('STATE_CHANGE', self, (ALLOCATING,))
                        self.__allocateSpace(length)
                        self.__allocatedBytes = length
                    except:
                        print sys.exc_info()
                        dispatcher.send('STATE_CHANGE', self, (FAILED, ALLOCATION_FAILED))
                        dispatcher.send('ERROR', 'subordinateManager.__tempFile', 'Failed during allocation...')
                        self.__taskQueue.popleft()
                        self.__doTask()
                        return # @TODO: Actually this breaks the flow.
                # set up new downloadThread
                self.dlThread = downloadThread(url, begin, length, self.__tempFile)
                self.dlThread.start()

            elif self.__taskQueue[0][0][0] == KILL:
                dispatcher.send('DEBUG', 'subordinateManager.taskQueue', 'DIE request.') 
                self.die()
            elif self.__taskQueue[0][0][0] == ABORT:
                dispatcher.send('DEBUG', 'subordinateManager.taskQueue', 'Request to abort transfer.')
                self.dlThread.cancel() 
            else:
                dispatcher.send('ERROR', 'subordinateManager.taskQueue', 'Unimplemented request.') 
                
            # remove finished task
            self.__taskQueue.popleft()
            self.__doTask()
                
    def getReport(self):
        return Report(self, self.dlThread).generateReport()

    def generateXML(self):
        return self.getReport()

    def getFile(self):
        if self.state == FINISHED:
            print "CALY"
            self.dlThread.tempFile.seek(0)
            return self.dlThread.tempFile
        print "NONE"
        return None
    
    def die(self):
        dispatcher.send('DEBUG', 'subordinateManager', 'Received a request to DIE, doing just so.')
        if self.dlThread is not None:
            self.dlThread.cancel()
            self.dlThread.join()             # wait until cancelled properly
        
        self.__tempFile.close()
        dispatcher.send('DEBUG', 'dwgetds', 'Exit.')
        self.__logger.writeLogFile()
        sys.exit(0)