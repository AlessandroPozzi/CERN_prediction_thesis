import mysql.connector
from File_writer import File_writer
from datetime import datetime
from datetime import timedelta
import math
import config

class ColumnStats(object):
    ''' Deals with columns statistics of 1 device '''
    def __init__(self, write, fw):
        self.statesDict = dict() #key = state, value = occurrences
        self.tagsDict = dict() 
        self.descriptionDict = dict()
        self.duplicates = 0
        self.fw = fw
        self.write = write
        self.deltaTimestamps = [] #list of temporal differences between the events of this device and the reference device
        self.msAverage = 99999999999999999
        self.msStandDev = 99999999999999999
        
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
        
    def writeState(self):
        for k in self.statesDict:
            result = str(self.statesDict[k]) + " - [  State   ] " + k
            result = result.encode('ascii', 'ignore').decode('ascii')
            if self.write:
                self.fw.write_txt(result)
            
    def writeTag(self):
        for k in self.tagsDict:
            result = str(self.tagsDict[k]) + " - [   Tag    ] " + k
            result = result.encode('ascii', 'ignore').decode('ascii')
            if self.write:
                self.fw.write_txt(result)
            
    def writeDescr(self):
        for k in self.descriptionDict:
            result = str(self.descriptionDict[k]) + " - [Description] " + k
            result = result.encode('ascii', 'ignore').decode('ascii')
            if self.write:
                self.fw.write_txt(result)
        
    def writeDuplicates(self):
        result = "Numero di dati falsati dalla doppia query (duplicati): " + str(self.duplicates)
        if self.write:
            self.fw.write_txt(result)
        
    def writeTemporalPosition(self):
        if len(self.deltaTimestamps) == 0:
            return #no reason to compute average and variance
        tsum = timedelta()
        for ts in self.deltaTimestamps:
            tsum += ts
        average = tsum / len(self.deltaTimestamps) #Average as a TimeDelta
        self.msAverage = average.total_seconds() * 1000 + average.microseconds / 1000 #Average in milliseconds
        if len(self.deltaTimestamps) > 1: #do not compute variance with only 1 data point:
            numerator = 0.000000000 # numerator of the variance formula
            for ts in self.deltaTimestamps:
                par = ts - average
                msPar = par.total_seconds() * 1000 + par.microseconds / 1000
                numerator += (msPar * msPar)
            variancems = numerator / (len(self.deltaTimestamps) - 1) #variance in milliseconds
            standDev = timedelta(milliseconds = math.sqrt(variancems)) #standard deviation as a timedelta
            self.msStandDev = math.sqrt(variancems) #standard deviation in milliseconds
        if self.write:
            resultAvg = "AVERAGE appearance after: " + str(average)
            resultVar = "STANDARD DEVIATION of appearances: " + str(standDev)
            self.fw.write_txt(resultAvg)
            self.fw.write_txt(resultVar)

def compareChosenDevicesByAlarmPriority(fileName, priority, device_extra, cursor, write):
   
    d = fileName
    l = priority
    fw = None
    flagCoupleRef = False
    if write:
        fw = File_writer(d, priority, "column", "analysis")
        fw.create_txt("../../output/columnAnalysis/")
        fw.write_txt('\nDEVICE '+ str(d) + ': ')
        fw.write_txt('\n\tPRIORITY ' + str(map("_".join(), l)) + ':')
    # Create the entries in the dictionary for the device to analyze:
    devicesDict = dict() # key = device, value = an object of the class "columnStats"
    for devExtra in device_extra:
        key = devExtra[0] + "--" + devExtra[1]
        devicesDict[key] = ColumnStats(write, fw) #create new object for the analysis
    #Check the extra in the config to select the proper index
    if config.EXTRA == "state":
        extraIndex = 2
    elif config.EXTRA == "tag":
        extraIndex = 3
    elif config.EXTRA == "description":
        extraIndex = 4
    # 3 cases for the query:
    if len(priority)==1:
        priorCond = "and livellopriorita=%s"
        query = ("select Device, Time, State, Tag, Description from electric where device=%s " + priorCond + " and action='Alarm CAME' order by time")
        cursor.execute(query, (d,l[0]))
    elif isinstance(fileName, tuple):
        #The reference device has the format (device,extra)
        extraCond = " and state=%s"
        query = ("select Device, Time, State, Tag, Description from electric where device=%s " + extraCond + " and action='Alarm CAME' order by time")
        cursor.execute(query, (fileName[0],fileName[1]))
        flagCoupleRef = True
    else:
        query = ("select Device, Time, State, Tag, Description from electric where device=%s and action='Alarm CAME' order by time")
        cursor.execute(query, (d,))
   
    events = cursor.fetchall()
    allSeenEvents = []
    
    
    for e in events:
        if flagCoupleRef:
            #if the reference device is a couple (device, extra)
            keyName = fileName[0] + "--" + fileName[1]
        else:
            #if the reference device is a standard string
            keyName = fileName + "--"
        if keyName not in devicesDict:
            devicesDict[keyName] = ColumnStats(write, fw)
        
        allSeenEvents.append((e[0], e[1]))
        query = ("select Device, Time, State, Tag, Description from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
        cursor.execute(query, (e[1], e[1], config.CORRELATION_MINUTES))
        eventsAfter = cursor.fetchall()
        
        if (e[0], e[1]) not in allSeenEvents:
            allSeenEvents.append((e[0], e[1]))
            devicesDict[keyName].updateState(e[2])
            devicesDict[keyName].updateTag(e[3])
            devicesDict[keyName].updateDescr(e[4])
        #else:
        #    devicesDict[e[0]].addDuplicate()
            
        for ea in eventsAfter:
            if config.EXTRA: #create the names with the "--"
                extraAscii = ea[extraIndex].encode('ascii', 'ignore').decode('ascii')
                extraAscii.replace("'", "")
                couple = (ea[0], extraAscii)
                devExtra = couple[0] + "--" + couple[1]
            else:
                couple = (ea[0], "")
                devExtra = couple[0] + "--"
            if couple in device_extra: #if the device-state couples is in the list of the device-states to analyze..

                if (ea[0], ea[1]) not in allSeenEvents: #consider each (deviceName,timestamp) couple only once
                    #Now update all the statistics:
                    allSeenEvents.append((ea[0], ea[1]))
                    devicesDict[devExtra].updateState(ea[2])
                    devicesDict[devExtra].updateTag(ea[3])
                    devicesDict[devExtra].updateDescr(ea[4])
                    devicesDict[devExtra].addTimestamp(e[1], ea[1])
                    #else:
                    #    devicesDict[ea[0]].addDuplicate()
                        
    # COMPUTE OCCURRENCES, AVERAGE...
    for k in devicesDict:
        if write:
            fw.write_txt("DEVICE " + k + ":", newline=True)
        devicesDict[k].writeState()
        devicesDict[k].writeTag()
        devicesDict[k].writeDescr()
        devicesDict[k].writeTemporalPosition()
        #devicesDict[k].writeDuplicates()
        
    return devicesDict

def find_column_distribution(fileName, priority, device_extra, write = False):
    ''' 
    Finds average and standard deviation (in milliseconds) of the "networkDevices" passed as parameter,
    w.r.t. to the given fileName and priority.
    fileName can be a string or a tuple (device,extra)
    device_extra is a list that contain couples (device, extra)
    write = True if you want to write in the txt file
    ----
    Returns a dictionary with "device--extra" as keys. The values are objects of the class "ColumnStats".
    '''
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    if not isinstance(priority, list):
        priority = [priority]
    devicesDict = compareChosenDevicesByAlarmPriority(fileName, priority, device_extra, cursor, write)
    cursor.close()
    return devicesDict
