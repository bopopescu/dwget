# coding=utf8
from pydispatch import dispatcher

from common.Logger import Logger
from threading import Thread as th
from dwgetd.threads import *
from dwgetd.Subordinate import *
from dwgetd.Task import *
from dwgetd.Subordinatedriver import *
from collections import deque
from common import consts
import socket, sys, time



class TaskAlreadyExists(Exception):
    pass


class Dwgetd(object):
    

    taskList = []
    taskQueue = deque()
    __thread = None 
    __driver = Subordinatedriver()
    replyMsg = ''
    
    def __init__(self):
        
        # init loggging
        logger = Logger('dwgetd',2001, '127.0.0.1', True)
        logger.addStuff(self.__driver)
    
        dispatcher.connect(self.newSubordinate, signal = 'NEW_SLAVE', sender = 'mainMainThread')
        dispatcher.connect(self.delSubordinate, signal = 'DEL_SLAVE', sender = 'mainMainThread')
        dispatcher.connect(self.newDownload, signal = 'NEW_DOWNLOAD', sender = 'mainMainThread')
        dispatcher.connect(self.__reply, signal = 'REPLY_SOCKET', sender = 'mainMainThread')
        dispatcher.connect(self.listSubordinates, signal = 'LIST_SLAVES', sender = 'mainMainThread')
        dispatcher.connect(self.listTasks, signal = 'LIST_TASKS', sender = 'mainMainThread')
        self.__thread = MainThread()
        self.__thread.start()
        
    def __reply(self, reply_sock, signal, sender):
        self.replyMsg += '\n'
        sent = 0
        while sent < len(self.replyMsg):
            sent += reply_sock.send(self.replyMsg[sent:])
            
    def newSubordinate(self, ip, signal, sender):
        
        if not self.__driver.addSubordinate(ip):
            self.replyMsg = 'Subordinate: %s added' % (ip)
        else:
            self.replyMsg = 'Subordinate: %s already exists' % (ip)
            dispatcher.send('ERROR', 'dwgetd', 'Subordinate: %s already exists' % (ip))
        
        
    def delSubordinate(self, ip, signal, sender):
        
        if not self.__driver.removeSubordinate(ip):
            self.replyMsg = 'Subordinate %s deleted form subordinate list' % (ip)
        else:
            self.replyMsg = 'No such subordinate: %s' % (ip)
            dispatcher.send('ERROR', 'dwgetd', 'Can\'t delete: there is no subordinate with ip: %s' % (ip))
    
    def listSubordinates(self, dummyArg):
        
        self.replyMsg = ''
        for subordinate in self.__driver.listSubordinates():
            self.replyMsg += (subordinate.ip + ' & ')
 
            
    def listTasks(self, dummyArg):
        self.replyMsg = ''
        for task in self.__driver.listTasks():
            self.replyMsg += (task.url + ' & ')

    def __newTask(self, url):
        findList = [task for task in self.taskList if task.url == url]
        if not len(findList):
            try: 
                task = Task(url, self.__driver)
                self.taskList.append(task)
                self.replyMsg = 'Task: %s added' % (url)
            except:
                 # TODO pass exception message (value) in python 2.5
                 print sys.exc_info()
                 print sys.exc_traceback
                 self.replyMsg = 'Task: %s not added, maybe wrong url?' % (url)
                 dispatcher.send('ERROR', 'dwgetd', 'Task: %s' % (url))
                 raise BadConnect
                
        else:
            self.replyMsg = 'Task: %s already exists' % (url)
            dispatcher.send('ERROR', 'dwgetd', 'Task: %s already exists' % (url))
            raise TaskAlreadyExists
        
        
    
    def newDownload(self, url, signal, sender):
        try:
            newTask = self.__newTask(url)
            self.taskQueue.append(newTask)
            #self.subordinateList[0].start((-1,0), url, self.taskList[0].file);
            #time.sleep(5)
            #self.subordinateList[0].updateReport();
            #time.sleep(10)
            #self.subordinateList[0].updateReport();
            #self.subordinateList[0].requestBinData();

        except:
            print sys.exc_info()
            print sys.exc_traceback
            dispatcher.send('ERROR', 'dwgetd', 'Problems with new download %s' % (url))

if __name__ == '__main__':

    dwgetd = Dwgetd()

    
    
        