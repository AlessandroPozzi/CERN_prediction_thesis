# USE THIS TO CHECK IF STATES, TAGS AND DESCRIPTIONS ARE MEANINGFUL 
#(IT IS CALLED AUTOMATICALLY WHEN THE MAIN IS EXECUTED)

import mysql.connector  # pip install mysql-connector-python
#from helpers.File_writer import File_writer
from File_writer import File_writer
from datetime import datetime
from datetime import timedelta
import math

class ColumnStats(object):
    ''' Deals with columns statistics of 1 device '''
    def __init__(self):
        self.statesDict = dict() #key = state, value = occurrences
        self.tagsDict = dict() 
        self.descriptionDict = dict()
        self.duplicates = 0
        self.deltaTimestamps = [] #list of temporal differences between the events of this device and the reference device
        
    def updateState(self, state):
        if state not in self.statesDict:
            self.statesDict[state] = 1
        else:
            self.statesDict[state] = self.statesDict[state] + 1
            
    def updateTag(self, tag):
        if tag not in self.tagsDict:
            self.tagsDict[tag] = 1
        else:
            self.tagsDict[tag] = self.tagsDict[tag] + 1
            
    def updateDescr(self, descr):
        if descr not in self.descriptionDict:
            self.descriptionDict[descr] = 1
        else:
            self.descriptionDict[descr] = self.descriptionDict[descr] + 1 
            
    def addTimestamp(self, tsReference, tsCurrent):
        ''' 
        tsReference = timestamp of the reference event
        tsCurrent = timestamp of event considered in this object
        '''
        ref = datetime.strptime(tsReference, '%Y-%m-%d %H:%M:%S.%f')
        cur = datetime.strptime(tsCurrent, '%Y-%m-%d %H:%M:%S.%f')
        self.deltaTimestamps.append(cur-ref)
            
    def addDuplicate(self):
        self.duplicates += 1
        
    def writeState(self, fw):
        for k in self.statesDict:
            result = str(self.statesDict[k]) + " - [  State   ] " + k
            result = result.encode('ascii', 'ignore').decode('ascii')
            fw.write_txt(result)
            
    def writeTag(self, fw):
        for k in self.tagsDict:
            result = str(self.tagsDict[k]) + " - [   Tag    ] " + k
            result = result.encode('ascii', 'ignore').decode('ascii')
            fw.write_txt(result)
            
    def writeDescr(self, fw):
        for k in self.descriptionDict:
            result = str(self.descriptionDict[k]) + " - [Description] " + k
            result = result.encode('ascii', 'ignore').decode('ascii')
            fw.write_txt(result)
        
    def writeDuplicates(self, fw):
        result = "Numero di dati falsati dalla doppia query (duplicati): " + str(self.duplicates)
        fw.write_txt(result)
        
    def writeTemporalPosition(self, fw):
        if len(self.deltaTimestamps) <= 1:
            return #no reason to compute average and variance
        tsum = timedelta()
        for ts in self.deltaTimestamps:
            tsum += ts
        average = tsum / len(self.deltaTimestamps)
        numerator = 0.000000000
        for ts in self.deltaTimestamps:
            par = ts - average
            parSec = par.total_seconds() #convert in seconds, lose microsecond accuracy for variance
            numerator += (parSec * parSec)
        varianceSec = numerator / (len(self.deltaTimestamps) - 1)
        standDev = timedelta(seconds = math.sqrt(varianceSec)) #standard deviation
        resultAvg = "AVERAGE appearance after: " + str(average)
        resultVar = "STANDARD DEVIATION of appearances: " + str(standDev)
        fw.write_txt(resultAvg)
        fw.write_txt(resultVar)

def compareChosenDevicesByAlarmPriority(fileName, priority, device_filtering, cursor):
          
    d = fileName
    l = priority
    fw = File_writer(d, priority, "column", "analysis")
    fw.create_txt("../../output/columnAnalysis/")
    fw.write_txt('\nDEVICE '+ str(d) + ': ')
    fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
    query = ("select Device, Time, State, Tag, Description from electric where device=%s and livellopriorita=%s and action='Alarm CAME'")
    cursor.execute(query, (d,l))
    events = cursor.fetchall()
    allSeenEvents = []
    devicesDict = dict() # key = device, value = an object of the class "columnStats"
    
    for e in events:
        if e[0] not in devicesDict:
            devicesDict[e[0]] = ColumnStats()
        
        allSeenEvents.append(e)
        query = ("select Device, Time, State, Tag, Description from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
        cursor.execute(query, (e[1], e[1], 5))
        eventsAfter = cursor.fetchall()
        
        if e not in allSeenEvents:
            allSeenEvents.append(e)
            devicesDict[e[0]].updateState(e[2])
            devicesDict[e[0]].updateTag(e[3])
            devicesDict[e[0]].updateDescr(e[4])
        #else:
        #    devicesDict[e[0]].addDuplicate()
            
        for ea in eventsAfter:
            
            if ea[0] in device_filtering or ea[0] == d:
                if ea[0] not in devicesDict:
                    devicesDict[ea[0]] = ColumnStats()
                    
                if ea not in allSeenEvents:
                    allSeenEvents.append(ea)
                    devicesDict[ea[0]].updateState(ea[2])
                    devicesDict[ea[0]].updateTag(ea[3])
                    devicesDict[ea[0]].updateDescr(ea[4])
                    devicesDict[ea[0]].addTimestamp(e[1], ea[1])
                #else:
                #    devicesDict[ea[0]].addDuplicate()
                    
                
    
    # PUT EVERYTHING IN THE TXT FILE
    for k in devicesDict:
        fw.write_txt("DEVICE " + k + ":", newline=True)
        devicesDict[k].writeState(fw)
        devicesDict[k].writeTag(fw)
        devicesDict[k].writeDescr(fw)
        devicesDict[k].writeTemporalPosition(fw)
        #devicesDict[k].writeDuplicates(fw)

def find_column_distribution(fileName, priority, networkDevices):
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    compareChosenDevicesByAlarmPriority(fileName, priority, networkDevices, cursor)
