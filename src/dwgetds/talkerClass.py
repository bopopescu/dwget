'''
Created on 2009-05-02

@author: ltylicki
'''

from socket import * 
from thread import * 

class communicationThread(Thread):
    '''
    Class @deprecated: another class methods implements needed functionality
    @see talkerThread.py.
    '''
    sock = None 
    client = None 
    address = None

    def waitForMaster(self): 
        self.sock = socket(AF_INET, SOCK_STREAM)  
        self.sock.bind(('', 6677)) 
        self.sock.listen(5) 
            
    def __init__(selfparams):
        pass 
    
    def run(self): 
        self.waitForMaster() 
        while True : 
            self.client, self.address = self.sock.accept() 
            print 'Polaczenie z ', self.address 
            self.client.send(time.ctime(time.time())) 
            self.client.close() 