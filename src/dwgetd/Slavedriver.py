'''
Created on May 28, 2009

@author: tk
'''
# coding=utf8
from common.consts import *
from dwgetd.Subordinate import *
from pydispatch import dispatcher
from math import floor
import thread

from xml.etree import ElementTree as ET
from xml.etree.ElementTree import *

class Subordinatedriver():
    '''
    Class which task is managing all of the subordinates that are assigned to the main.
    It handles the global subordinate list and it adds new subordinates, removes subordinates,
    controls the subordinates that has been disconnected or have been timeouted.
    It checks whether the subordinates are still connected. It counts the subordinate downloading
    rates.
    '''
    __subordinateList = []
    __taskList = []
    __lock = None

    def getLock(self):
        res = self.__lock.acquire(1)
        return res
    
    def releaseLock(self):
        return self.__lock.release()

    def __init__(self):
        self.__lock=thread.allocate_lock()
        dispatcher.connect(self.parseRequest, signal = 'TASK_REQUEST', sender = dispatcher.Any)
        dispatcher.connect(self.fragmentCompleted, signal = 'TASK_COMPLETE', sender = dispatcher.Any)
        dispatcher.connect(self.removeDeadSubordinate, signal='SLAVE_DEAD', sender = dispatcher.Any)
        #dispatcher.connect(self.removeDuplicates, signal='DONE_CHUNK', sender = dispatcher.Any)

    def removeDeadSubordinate(self, arg, signal, sender):
        self.removeSubordinate(arg.ip)
        self.doVoodoo() 
        
    def removeDuplicates(self, arg, ip):
        for subordinate in self.__subordinateList:
            if subordinate.fileFragment == arg:
                if subordinate.isActive and subordinate.ip != ip:
                    print "FOUND DUPLICATED CHUNK TO ABORT ", subordinate.ip , subordinate.isActive
                    print arg.offset
                    print subordinate.fileFragment.offset
                    subordinate.abort()
                    self.doVoodoo()
        
    def addSubordinate(self, ip):
        # @TODO should doVoodoo.
        findList = [subordinate for subordinate in self.__subordinateList if subordinate.ip == ip]
        if not len(findList):
            self.__subordinateList.append(Subordinate(ip, self))
            self.doVoodoo()
            return 0
        else:
            dispatcher.send('ERROR', 'dwgetd', 'Subordinate: %s already exists' % (ip))
            return -1
            
    def removeSubordinate(self, ip):
        toDelList = [subordinate for subordinate in self.__subordinateList if subordinate.ip == ip]
        for subordinate in self.__subordinateList: 
            print subordinate.ip
        if len(toDelList):
            # @TODO: dispatch subordinates task to other subordinates.
            print "!!!!!!!!!!!!!!!!!!!!!!!"
            print "!!!!!!!!!!!!!!!!!!!!!!!"
            print toDelList[0].ip 
            print ip 
            print "!!!!!!!!!!!!!!!!!!!!!!!"
            print "!!!!!!!!!!!!!!!!!!!!!!!"
            print toDelList[0] in self.__subordinateList 
            self.__subordinateList.remove(toDelList[0])
            print "Po usunieciu: "
            for subordinate in self.__subordinateList: 
                print subordinate.ip 
            return 0
        else:
            dispatcher.send('ERROR', 'dwgetd', 'Can\'t delete: there is no subordinate with ip: %s' % (ip))
            return -1

    def listSubordinates(self):
        return self.__subordinateList

    def listTasks(self):
        return self.__taskList
    
    def parseRequest(self, arg, signal, sender):
        if arg == NEW_TASK:
            self.newTask(sender)
        else:
            dispatcher.send('ERROR', 'subordinatedriver', 'Unknown request.')

    def rateSpeed(self): 
        #print "Oceny dla subordinate'ow:" 
        for i in self.__subordinateList: 
            #print "average speed: &f" %i.avgSpeed 
            subordinatesCnt = 0 
            avgSpeed = 0 
            #i.rating = 0.0 
            for j in self.__subordinateList: 
                if i.url == j.url: 
                    #i.rating += j.avgSpeed 
                    subordinatesCnt += 1 
                    avgSpeed += j.avgSpeed
            if subordinatesCnt > 0 and avgSpeed > 0: 
                i.rating = i.avgSpeed / (avgSpeed / subordinatesCnt) 
            else:
                i.rating = 1.
            #print i.url + " %f" %i.rating 
        #print "Koniec ocen dla subordinate'ow..." 

    def newTask(self, task):
        # @TODO: remove duplicates
        # @TODO: start task
        dispatcher.send('DEBUG', 'subordinatedriver', 'New task started.')
        self.__taskList.append(task)
        self.doVoodoo()

    def fragmentCompleted(self, arg, signal, sender):
        self.doVoodoo()
        
    def taskCompleted(self, task):
        task.status = TASK_FINISHED
        #self.__taskList.remove(task)
    
    def doVoodoo(self):
                
        while not self.__lock: # waiting for lock initialization
            pass
        
        #print "RECALCULATING ENVIRONMENT"
        self.getLock()
        
        self.rateSpeed()
        
        for task in self.__taskList: 
            if task.status > 1: 
                continue 
            for chunk in task.fileFragments:
                if chunk.done != 0: continue # don't waste time on not-being-downloaded chunks 
                ip = chunk.ip
                if len([subordinate for subordinate in self.__subordinateList if subordinate.ip == ip]) == 0 : # using dead subordinate
                    print task.url, chunk.ip, chunk.offset, "FAILED"
                    chunk.setRetry()
            
        for subordinate in self.__subordinateList:
            if not subordinate.isActive and not subordinate.isDead:
                for task in self.__taskList:
                    if task.status > 1:             # Don't waste time thinking about a completed task.
                        continue
                    if task.fileFragments:          # Any to be retried after subordinate failure?
                        for chunk in task.fileFragments:
                            if chunk.isToBeDownloaded():    # == FF_ERROR || FF_NEW
                                print task.url, subordinate.ip, chunk.offset, " IS BEING DOWNLOADED"
                                subordinate.setFileFragment(chunk)
                                subordinate.start((chunk.length+1, chunk.offset-1), task.url, task.file)
                                chunk.ip = subordinate.ip
                                
                                # @TODO: self.doVoodoo() if it starts locking.
                                self.releaseLock()
                                self.doVoodoo()
                                return

                    offset = 0
                    if task.fileFragments:
                        for chunk in task.fileFragments:
                            offset = max(offset, chunk.offset+chunk.length) # Last chunks offset.
                            
                    if (offset+1) < task.size:                              # If last chunk happens to be outside the file.
                        print task.url, task.supportsResume
                        if task.supportsResume:
                            chunk = task.addFileFragment(offset+1, min(max(int(floor(subordinate.rating*basePartSize)), minPartSize), task.size-offset-1), subordinate.ip)
                            print task.url, subordinate.ip, chunk.offset,  chunk.length, " IS BEING DOWNLOADED"
                            subordinate.setFileFragment(chunk)
                            subordinate.start((chunk.length+1, chunk.offset-1), task.url, task.file)
                        else:
                            subordinate.setFileFragment(task.addFileFragment(0, task.size, subordinate.ip))
                            subordinate.start((task.size, 0), task.url, task.file)
                            print task.url, subordinate.ip, chunk.offset, " IS BEING DOWNLOADED"
                            
                        self.releaseLock()
                        self.doVoodoo()
                        return
                        #continue
                    else:
                        allDone = True
                        if task.fileFragments:
                            print task.url, task.size
                            for chunk in task.fileFragments:
                                if not chunk.done:
                                    allDone = False
                                print chunk.offset, chunk.done

                        if allDone and (task.status != TASK_MERGING):
                            # @TODO in a different thread!
                            task.mergeFile()
                            continue
                        #sys.exit(0)
               # subordinate.start(())
               
               # If we're here, we don't have anything to do. Can't let that happen, can we...
               # 
               
#                ratingToBeat = subordinate.rating
#                worstTask = None
#                worstChunk = None
#                worstRank = 0               
#                for task in self.__taskList:
#                    if task.status > 1:             # Don't waste time thinking about a completed task.
#                        continue
#                    if task.fileFragments:          # Any to be retried after subordinate failure?
#                        for chunk in task.fileFragments:
#                            if chunk.done != 0:
#                                continue
#                            
#                            remaining = 0
#                            avgSpeed = 0
#                            rating = 0
#                            
#                            for subordinate in self.__subordinateList:
#                                if chunk.ip == subordinate.ip:
#                                    remaining = chunk.length - subordinate.received  
#                                    avgSpeed = subordinate.avgSpeed
#                                    rating = subordinate.rating
#                                    
#                            rank = remaining*avgSpeed*rating
#                            
#                            if rank > worstRank:
#                                worstRank = rank
#                                worstTask = task
#                                worstChunk = chunk
#                    
#                    print task.url, subordinate.ip, chunk.offset,  chunk.length, "IS BEING DUPLICATED (TAKEN OVER FROM", chunk.ip, ")."
#                    subordinate.setFileFragment(chunk)
#                    subordinate.start((chunk.length+1, chunk.offset-1), task.url, task.file)
#               
        self.releaseLock()
        
    def generateXML(self):
        '''
        Method generates the XML report of everything for Django frontend.        
        
        @return ready XML string
        '''
        
        doc = Element("DWGETD")
        tasks = SubElement(doc, 'TASKS')
        for tsk in self.__taskList:
            task = SubElement(tasks, 'TASK')
            SubElement(task, u'url').text = '%s' % (tsk.url)
            SubElement(task, u'size').text = '%d' % (tsk.size)
            SubElement(task, u'status').text = '%d' % (tsk.status)            
            if tsk.fileFragments:
                chunks = SubElement(task,'CHUNKS')
                for chnk in tsk.fileFragments:
                    chunk = SubElement(chunks,'CHUNK')
                    SubElement(chunk, u'offset').text = '%d' % (chnk.offset)
                    SubElement(chunk, u'length').text = '%d' % (chnk.length)
                    SubElement(chunk, u'ip').text = '%s' % (chnk.ip)
                    SubElement(chunk, u'done').text = '%d' % (chnk.done)
        
        return ET.tostring(doc, 'utf-8')
        