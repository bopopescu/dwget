'''
Created on 2009-05-02

@author: ltylicki
'''

from pydispatch import dispatcher 

from socket import * 
from threading import Thread 
from common.Logger import Logger 
from common.consts import * 
#from xml.etree import * 
from xml.etree import ElementTree 
import sys
from xml.etree.ElementTree import *

class talkerThread(Thread):
    """
    Class which is used to do whole of the communication with the main. It is used as
    the middleman between main and the subordinate. It parsses the requests sent in XML 
    format from the main and dispatches messages to the subordinate. It is also used to 
    send the complete file fragment to the main.
    """ 
    sock = None 
    client = None 
    address = None 
    msg = ''
    
    subordinateManager = None 
    
    requestType = None 
    url = None 
    begining = -1 
    length = -1 
    interface = ''

    def initSocket(self): 
        self.sock = socket(AF_INET, SOCK_STREAM) 
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)  
        self.sock.bind((self.interface, 6678)) 
        self.sock.listen(5) 
        
    def waitForMain(self): 
        self.client, self.address = self.sock.accept() 
        dispatcher.send('DEBUG', 'talkerThread', 'Main has connected...') 
        
    def __init__(self, subordinateManager, interface=''): 
            Thread.__init__(self) 
            self.interface = interface
            self.subordinateManager = subordinateManager 
    
    def parseMainRequest(self, data):  
#        @DEBUG
#        print "RECEIVED: ", data 
#        print '\n' 
#        

        try:
            xmlMessage = XML(data) 
            requestType = xmlMessage.get("type") 
            if requestType == 'start' : 
                url = xmlMessage.find("url").text 
                length = int(xmlMessage.find("length").text) 
                begining = int(xmlMessage.find("start").text) 
                dispatcher.send('DEBUG', 'talkerThread', 'Main has requested a file download...') 
                dispatcher.send('MASTER_REQUEST', self, (NEW_URI, url, begining, length)) 
                self.sendFileToMain(self.generateClientResponseXML(True), False) 
            elif requestType == 'upload': 
                dispatcher.send('DEBUG', 'talkerThread', 'Main has requested a file upload...')
                self.sendFileToMain(self.subordinateManager.getFile(), True) 
            elif requestType == 'abort': 
                dispatcher.send('DEBUG', 'talkerThread', 'Main has requested an abortion of file downloading...') 
                dispatcher.send('MASTER_REQUEST', self, (ABORT,)) 
                self.sendFileToMain(self.generateClientResponseXML(True), False) 
            elif requestType == 'report': 
                dispatcher.send('DEBUG', 'talkerThread', 'Main has requested a download report...')
                self.sendFileToMain(self.subordinateManager.getReport(), False) 
            elif requestType == 'kill': 
                dispatcher.send('DEBUG', 'talkerThread', 'Main has requested killing a daemon...')          
                dispatcher.send('MASTER_REQUEST', self, (KILL,)) 
                self.sendFileToMain(self.generateClientResponseXML(True), False) 
            else : 
                dispatcher.send('DEBUG', 'talkerThread', 'Main has requested an unknown operation...')  
                self.sendFileToMain(self.generateClientResponseXML(False), False)
        except:
            print sys.exc_info()
            print "TALKERTHREAD: EXCEPTION"
    
    def sendFileToMain(self, file, binary): 
        if file == None: 
            dispatcher.send('DEBUG', 'talkerThread', 'Main has requested a incomplete part of binary file...')
        else : 
            if binary == False: 
                file += '\n' 
                sent = 0 
                while sent < len(file): 
                    sent += self.client.send(file[sent:]) 
                dispatcher.send('DEBUG', 'talkerThread', 'Sent main a complete part of XML file...')
            else:
#                file.seek(-1, 2)
                toUpload = self.subordinateManager.dlThread.length
                print "Uploading %d bytes." %(toUpload)
                file.seek(0)
                
                while toUpload > 0:
                    msg = file.read(min(128000, toUpload))
                    toUpload -= len(msg)
                    sent = 0
                    while sent < len(msg): 
                        sent += self.client.send(msg[sent:]) 

                self.subordinateManager.status = 666
                print "STATUS UPLOADING"
                        
                dispatcher.send('DEBUG', 'talkerThread', 'Sent main a complete binary file...')
                # @TODO: After a successful upload change status to non-6 
                  
    
    def generateClientResponseXML(self, isOK): 
        doc = Element("Response") 
        if isOK == True: 
            SubElement(doc, u'status').text = 'OK' 
        else: 
            SubElement(doc, u'status').text = 'Unknown Command' 
        return tostring(doc, 'utf-8') 
    
    def run(self): 
        self.initSocket() 
        while True : 
            self.waitForMain() 
            self.msg = '' 
            while True : 
                data = self.client.recv(512) 
                self.msg = self.msg + data 
                
                if data[-1] == '\n': break 
            
            self.parseMainRequest(self.msg) 
             
            self.client.close() 
        self.sock.close() 
         