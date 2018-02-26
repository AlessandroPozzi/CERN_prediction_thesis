'''
Created on 14 feb 2018

@author: Alessandro Pozzi
'''
import mysql.connector
from File_writer import File_writer
import config
import columnAnalyzer as colAnal
import re
from scipy.spatial.distance import correlation

class DatabaseNetworkCorrelator(object):
        
    def initAsPostProcessor(self, nh, gh, log):
        ''' 
        After creating the object, call this method to use the class as a post-processor (i.e. to 
        perform an analysis on the devices of the network while having at disposal the Network_handler,
        Data_extractor, etc).
        ---- Parameters ----
        nh = networkHandler
        gh = generalHandler
        log = True if you want to print some debug info in the console
        '''
        self.edgeLabels = nh.edgeLabels #(edge[0], edge[1], value, value_inv)
        self.gh = gh
        self.log = log
        self.devicesColumnDict = nh.devicesColumnDict #key=device, value=an object of the class ColumnStats in the columnAnalyzer
        self.referenceDevice = nh.device_considered_realName
        self.rankedDevices = nh.rankedDevices #tuple (device, occurrences, frequency, average, stand.dev.) or (device, occurrences, frequency, "lift:", lift)
        self.priority = nh.priority_considered
        self.devices = nh.best_model.nodes() #the nodes of the network (already created)
        self.itemsetOccurrences = nh.occurrences #it may include duplicates (depends by the itemset generation method)
        self.noDupItemsetOccurrences = dict() #occurrences in the 5 min itemsets without duplicates
        self.fw = File_writer(self.referenceDevice, self.priority, "postProcessingAnalysis")
        self.fw.create_txt("../../output/postProcessingAnalysis/")
        self.fw.write_txt("File of device " + self.referenceDevice + " with priority " + self.priority + " (POST processing)")
        self.post = True
        
    def initAsPreProcessor(self, refDevice, priority, log = False):
        '''
        After creating the object, call this method to use the class as a pre-processor (i.e. to
        select the variables to use in the network, without having the Network_handler yet).
        '''
        self.log = log
        self.post = False
        self.referenceDevice = refDevice
        self.priority = priority
        self.noDupItemsetOccurrences = dict()
        self.fw = File_writer(self.referenceDevice, self.priority, "preProcessingAnalysis")
        self.fw.create_txt("../../output/preProcessingAnalysis/")
        self.fw.write_txt("File of device " + self.referenceDevice + " with priority " + self.priority + " (PRE network creation)")
    
    def __totalOccurrencesAnalysis(self, devices):
        ''' 
        This should not be called directly. Use totalOccurrencesCandidatesAnalysis
        or totalOccurrencesNetworkAnalysis methods instead. 
        This method finds the TOTAL occurrences in the database of each of the given devices, then computes how many times 
        each given device was found n minutes (config.CORRELATION_MINUTES) after the reference device.
        The ratio between this two numbers is the LIFT, which is computed as a percentage and returned.
        '''
        # Open a connection to the database:
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        
        # Count the total appearances of the devices in the database
        self.occurrencesDB = self.__countEventDatabase(devices)
        
        # Count the relative occurrences (after n minutes) wrt to the reference device:
        self.noDupItemsetOccurrences = self.__relativeOccurrences(devices, moreThanOnce = True)
        
        # Compute the lift:
        lift = dict()
        for d in devices:
            lift[d] = round(self.noDupItemsetOccurrences[d] / float(self.occurrencesDB[d]), 2) * 100
                 
        #Save the data in the txt file:
        self.fw.write_txt("Analysis of the occurrences of the devices: " + str(devices), True)
        self.fw.write_txt("DEVICE | PARTIAL | TOTAL | LIFT")
        rankings = []
        for d in devices:
            couple = (d, lift[d])
            rankings.append(couple)
        rankings.sort(key = lambda tup: tup[1], reverse=True)
        for tpl in rankings:
            d = tpl[0]
            lft = tpl[1]
            self.fw.write_txt(d + "\t" + str(self.noDupItemsetOccurrences[d]) + "\t" + 
                                str(self.occurrencesDB[d]) + "\t" + str(lft))
            
        #If this is object is being used to find the candidates nodes of the network, return the lift:
        if self.post == False:
            return lift
        
    def totalOccurrencesNetworkAnalysis(self):
        ''' 
        Finds the TOTAL occurrences in the database of each of the devices in the Bayesian network, then computes how many times 
        each given device was found n minutes (config.CORRELATION_MINUTES) after the reference device.
        The ratio between this two numbers is the LIFT, which is computed as a percentage and saved in the class' txt file. 
        '''
        self.__totalOccurrencesAnalysis(self.devices)
    
    def totalOccurrencesCandidatesAnalysis(self, candidates = []):
        '''
        Finds the TOTAL occurrences in the database of each of the given devices, then computes how many times 
        each given device was found n minutes (config.CORRELATION_MINUTES) after the reference device.
        The ratio between this two numbers is the LIFT, which is computed as a percentage, saved in the class' txt file and 
        returned. 
        The idea of this method is to use the lift to select the variables to use in the Bayesian network
        '''
        if not candidates:
            for tpl in self.rankedDevices:
                candidates.append(tpl[0])
        return self.__totalOccurrencesAnalysis(candidates)
    
    def __countEventDatabase(self, devices):
        '''
        Counts all the occurrences of the events of the given devices in the database. 
        Returns a dictionary (key = device, value = occorrences).
        '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        occurrencesDB = dict() #key = device, value = occurrences
        for d in devices:
            if self.log:
                print("Processing device number " + str(devices.index(d) + 1) + "/" + str(len(devices)))
            # Find the total occurrences of the (network) devices in the DB:
            query = "select COUNT(*) from cern.electric where device= %s and action='Alarm CAME'"
            cursor.execute(query, (d,))
            events = cursor.fetchall()
            occurrencesDB[d] = events[0][0]
        return occurrencesDB
    

    def __relativeOccurrences(self, devices, refDevice = None, priorityCond = True, moreThanOnce = False, correlationUniquness = None):
        '''
        Find the real itemset occurrences (with no duplicates) of the devices after n (config.CORRELATION_MINUTES) minutes.
        Basically, it is how many times each given device (in devices list) was found n minutes (config.CORRELATION_MINUTES) 
        after the reference device.
        Notes:
        - If onlyOnce = True then each event AFTER is considered only once.
        - If config.CORRELATION_UNIQUENESS is True each AFTER event is counted only once per reference event
        - Tn order to compute correctly the "relevance" (i.e. #eventsOfD2afterRefDevice / #eventsOfRefDevice) you should have 
          moreThanOnce = True and config.CORRELATION_UNIQUENESS = True
        '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        if refDevice == None:
            refDevice = self.referenceDevice
        if correlationUniquness == None:
            correlationUniquness = config.CORRELATION_UNIQUENESS
        if priorityCond: # Use the query with the priority
            query = ("select Device, Time, id from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
            cursor.execute(query, (refDevice, self.priority))
        else: # ignore priority: select event from all priority levels
            query = ("select Device, Time, id from electric where device=%s and action='Alarm CAME' order by time")
            cursor.execute(query, (refDevice,) )
                        
        allSeenEvents = []
        events = cursor.fetchall()
        noDupItemsetOccurrences = dict()
        for e in events:
            strList = "%s" # The strList is used to select only the devices in the network
            for i in range(1, len(devices)):
                strList = strList + " OR device=" + "%s"
            # Query to find the events n minutes later
            query = ("select Device, Time, Id from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' and (device=" 
                     + strList + ") order by time;")
            tpl = (e[1], e[1], config.CORRELATION_MINUTES) + tuple(devices)
            cursor.execute(query, tpl)
            eventsAfter = cursor.fetchall()
            uniqueness = []
            # Now we count a device occurrence without duplicating events 
            for ea in eventsAfter:
                if ea not in allSeenEvents or moreThanOnce: #each event will be considered exactly once (unless moreThanOnce is True)
                    allSeenEvents.append(ea)
                    if correlationUniquness: #if we want to count the event after only once per reference event
                        if ea[0] not in uniqueness:
                            uniqueness.append(ea[0])
                            if ea[0] not in noDupItemsetOccurrences: #key already present
                                noDupItemsetOccurrences[ea[0]] = 1
                            else:
                                noDupItemsetOccurrences[ea[0]] = noDupItemsetOccurrences[ea[0]] + 1
                    else:
                        if ea[0] not in noDupItemsetOccurrences: #key already present
                            noDupItemsetOccurrences[ea[0]] = 1
                        else:
                            noDupItemsetOccurrences[ea[0]] = noDupItemsetOccurrences[ea[0]] + 1
        return noDupItemsetOccurrences
    
    
    def __findDoublePresence(self, dev1, dev2, devRef, priorRef):
        ''' 
        Finds how many times the devices dev1 and dev2 were found TOGETHER after the events of the reference
        device devRef with priority priorRef. Then it does the same without considering the priority.
        Returns these two values and also the number of events of devRef.
        '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        query = ("select Device, Time, id, livellopriorita from electric where device=%s and action='Alarm CAME' order by time")
        cursor.execute(query, (devRef,) )
                        
        allSeenEvents = []
        events = cursor.fetchall()
        couplesRightPriority = 0
        couplesAnyPriority = 0
        for e in events:
            strList = "%s OR device=" + "%s"
            # Query to find the events n minutes later
            query = ("select Device, Time, Id from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' and (device=" 
                     + strList + ") order by time;")
            tpl = (e[1], e[1], config.CORRELATION_MINUTES, dev1, dev2)
            cursor.execute(query, tpl)
            eventsAfter = cursor.fetchall()
            # Now we count a device occurrence without duplicating events
            partialRightPriorOccurrencesDev1 = 0
            partialRightPriorOccurrencesDev2 = 0
            partialAnyPriorOccurrencesDev1 = 0
            partialAnyPriorOccurrencesDev2 = 0
            for ea in eventsAfter:
                if ea not in allSeenEvents: #each event will be considered exactly once (unless moreThanOnce is True)
                    allSeenEvents.append(ea)
                    if e[3] == priorRef:
                        if ea[0] == dev1:
                            partialRightPriorOccurrencesDev1 += 1
                            partialAnyPriorOccurrencesDev1 += 1
                        elif ea[0] == dev2:
                            partialRightPriorOccurrencesDev2 += 1
                            partialAnyPriorOccurrencesDev2 += 1 
                    else:
                        if ea[0] == dev1:
                            partialAnyPriorOccurrencesDev1 += 1
                        elif ea[0] == dev2:
                            partialAnyPriorOccurrencesDev2 += 1 
            couplesRightPriority = couplesRightPriority + min(partialRightPriorOccurrencesDev1, partialRightPriorOccurrencesDev2)
            couplesAnyPriority = couplesAnyPriority + min(partialAnyPriorOccurrencesDev1, partialAnyPriorOccurrencesDev2)
                   
        return couplesRightPriority, couplesAnyPriority, len(events)
    
    def __findSameTimestampOccurences(self, dev1, dev2):
        ''' Returns the number of times in which dev1 was found at the same timestamp of dev2 '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        query = ("select Device, Time, id from electric where action='Alarm CAME' and (device=%s) order by Time;")
        tpl = (dev1, )
        cursor.execute(query, tpl)
        events = cursor.fetchall()
        sameTime = 0
        for e in events:
            query = ("select count(*) from electric where time=(%s) and action='Alarm CAME' and device= %s;")
            tpl = (e[1], dev2)
            cursor.execute(query, tpl)
            counts = cursor.fetchall()
            sameTime += counts[0][0]
        return sameTime
    
    def checkGeneralCorrelation(self):
        ''' Checks if the devices in the network are correlated (i.e. appear together) also in the rest of the DB '''
        # First, we must find the set of devices that are correlated by an high single (or double) activation
        self.fw.write_txt("GENERAL CORRELATIONS:", True)
        self.fw.write_txt("Node1 --> label1 | label2 --> Node2: AVG & ST.DEV. of Node2 w.r.t. Node1 (reference events)")
        self.fw.write_txt("")
        
        for el in self.edgeLabels:

            # Total events in database (only Alarm Came):
            occurrencesDB = self.__countEventDatabase([el[0], el[1]])
            if occurrencesDB[el[0]] >= 4000 or occurrencesDB[el[1]] >= 4000:
                self.fw.write_txt("Correlations between " + el[0] + "(" + str(occurrencesDB[el[0]])
                                  + " total occurreces) and " + el[1] + "(" + 
                                  str(occurrencesDB[el[1]]) + " total occurrences) not analyzed: there are too many events.")
                self.fw.write_txt("")
                continue
            # Average and standard deviation w.r.t. el[0] (ref.device):
            devicesDict0 = colAnal.find_column_distribution(el[0], ["L0", "L1", "L2", "L3"], el[1])
            # Average and standard deviation w.r.t. el[1] (ref.device):
            devicesDict1 = colAnal.find_column_distribution(el[1], ["L0", "L1", "L2", "L3"], el[0])
            # Relative events n minutes after el[0] (ref.device):
            noDupItemsetOccurrences0 = self.__relativeOccurrences([el[1]], el[0], priorityCond = False, moreThanOnce = True)
            # Relative events n minutes after el[1] (ref.device):
            noDupItemsetOccurrences1 = self.__relativeOccurrences([el[0]], el[1], priorityCond = False, moreThanOnce = True)
            # Relative events n minutes after el[0] (ref.device) WITHOUT UNIQUENESS:
            dupItemsetOccurrences0 = self.__relativeOccurrences([el[1]], el[0], priorityCond = False, moreThanOnce = True, correlationUniquness = False)
            # Relative events n minutes after el[1] (ref.device) WITHOUT UNIQUENESS:
            dupItemsetOccurrences1 = self.__relativeOccurrences([el[0]], el[1], priorityCond = False, moreThanOnce = True, correlationUniquness = False)
            # Relative events n minutes after el[0] (ref.device) WITHOUT UNIQUENESS AND WITHOUT DUPLICATES:
            realItemsetOccurrences0 = self.__relativeOccurrences([el[1]], el[0], priorityCond = False, moreThanOnce = False, correlationUniquness = False)
            # Relative events n minutes after el[1] (ref.device) WITHOUT UNIQUENESS AND WITHOUT DUPLICATES:
            realItemsetOccurrences1 = self.__relativeOccurrences([el[0]], el[1], priorityCond = False, moreThanOnce = False, correlationUniquness = False)
            # How many times the couples were found together after the reference event:
            couplesRightPriority, couplesAnyPriority, totRefEvents = self.__findDoublePresence(el[0], el[1], 
                                                                                               self.referenceDevice, 
                                                                                               self.priority)
            # Find how many times dev1 and dev2 had the same timestamp:
            sameTimeOccurrences = self.__findSameTimestampOccurences(el[0], el[1])
            if not noDupItemsetOccurrences0 or not noDupItemsetOccurrences1:
                self.fw.write_txt("No correlations between this two devices.")
                self.fw.write_txt("")
                continue
            # SHOW CORRELATIONS IN TXT
            self.fw.write_txt(el[0] + "--> " + str(el[2]) + "|" + str(el[3]) + " --> " + el[1])
            self.fw.write_txt("------ " + el[0] + " and " + el[1] + " were found together in " + 
                              str(couplesRightPriority) + "/" + str(totRefEvents) + " events of " +self.referenceDevice
                              + " (priority " + self.priority + ")")
            self.fw.write_txt("------ " + el[0] + " and " + el[1] + " were found together in " + 
                              str(couplesAnyPriority) + "/" + str(totRefEvents) + " events of " +self.referenceDevice
                              + " (any priority)")
            self.fw.write_txt("------ Occurrences at the same timestamp: " + str(sameTimeOccurrences))
            
            self.fw.write_txt("Does " + el[1] + " generally activate AFTER an event of " + el[0] + "?")
            self.fw.write_txt("------ AVG " + str(round(devicesDict0[el[1]].msAverage / 1000,2)) + " seconds & ST.DEV. " + 
                              str(round(devicesDict0[el[1]].msStandDev / 1000, 2)) + " seconds")
            self.fw.write_txt("------ " + el[0] + ": " + str(occurrencesDB[el[0]]) + " events ; " + 
                              el[1] +  ": " + str(occurrencesDB[el[1]]) + " events")
            self.fw.write_txt("------ " + 
                              str(noDupItemsetOccurrences0[el[1]]) + " times " + el[1] + " was found (AT LEAST ONCE) within " + 
                              str(config.CORRELATION_MINUTES) + " minutes after a " + el[0] + " event.")
            self.fw.write_txt("------ " + 
                              str(dupItemsetOccurrences0[el[1]]) + " times " + el[1] + " was found (IN TOTAL, WITH DUPLICATES) within " + 
                              str(config.CORRELATION_MINUTES) + " minutes after a " + el[0] + " event.")
            self.fw.write_txt("------ " + 
                  str(realItemsetOccurrences0[el[1]]) + " times " + el[1] + " was found (IN TOTAL, WITHOUT DUPLICATES) within " + 
                  str(config.CORRELATION_MINUTES) + " minutes after a " + el[0] + " event.")
            #self.fw.write_txt("------ Overlapping reference events of " + el[0] + " : " + str(dup0))
            '''
            self.fw.write_txt("------ Confidence: " + #(correlations / not_overlapping_reference_devices)
                              str(round((noDupItemsetOccurrences0[el[1]] / float(occurrencesDB[el[0]])) * 100, 2))
                              + "%")
            '''
            self.fw.write_txt("Does " + el[0] + " generally activate AFTER an event of " + el[1] + "?")
            self.fw.write_txt("------ AVG " + str(round(devicesDict1[el[0]].msAverage / 1000,2)) + " seconds & ST.DEV. " + 
                              str(round(devicesDict1[el[0]].msStandDev / 1000,2)) + " seconds")
            self.fw.write_txt("------ " + el[0] + ": " + str(occurrencesDB[el[0]]) + " events ; " + 
                              el[1] +  ": " + str(occurrencesDB[el[1]]) + " events")
            self.fw.write_txt("------ " + str(noDupItemsetOccurrences1[el[0]]) + " times " + el[0] + " was found (AT LEAST ONCE) within " + 
                              str(config.CORRELATION_MINUTES) + " minutes after a " + el[1] + " event.")
            self.fw.write_txt("------ " + 
                              str(dupItemsetOccurrences1[el[0]]) + " times " + el[0] + " was found (IN TOTAL, WITH DUPLICATES) within " + 
                              str(config.CORRELATION_MINUTES) + " minutes after a " + el[1] + " event.")
            self.fw.write_txt("------ " + 
                  str(realItemsetOccurrences1[el[0]]) + " times " + el[0] + " was found (IN TOTAL, WITHOUT DUPLICATES) within " + 
                  str(config.CORRELATION_MINUTES) + " minutes after a " + el[1] + " event.")
            #self.fw.write_txt("------ Overlapping reference events of " + el[1] + " : " + str(dup1))
            '''
            self.fw.write_txt("------ Confidence: " + #(correlations / not_overlapping_reference_devices)
                              str(round((noDupItemsetOccurrences1[el[0]] / float(occurrencesDB[el[1]])) * 100, 2))
                              + "%")
            '''
            self.fw.write_txt("The correlation between the 2 devices was found to be generally valid (without the reference device) " + 
                  str(realItemsetOccurrences1[el[0]] + realItemsetOccurrences0[el[1]] - sameTimeOccurrences * 2 - couplesAnyPriority * 2) + " times")
            self.fw.write_txt("")
                    
                    
    def findInterestingDevices(self):
        ''' Finds the devices (in all the database) with the highest number of events after "config.CORRELATION_MINUTES" minutes. '''
        num_lines = sum(1 for line in open('../../res/CERNdevices.txt'))
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        curr = 0
        with open ('../../res/CERNdevices.txt', 'r') as in_file:
            devicesInterest = [] #list of tuples of the type (device, numberOfEventsAfterXminutes)
            for line in in_file.readlines(): #each line is a CERN device
                curr += 1
                print(str(curr) + "/" + str(num_lines))
                dev = line.replace("\n","")
                eventsAfterXminutes = 0
                allSeenEvents = set()
                query = ("select Device, Time, id from electric where device=%s and action='Alarm CAME' order by time")
                cursor.execute(query, (dev,))
                events = cursor.fetchall()
                for e in events:
                    allSeenEvents.add(e)
                    query = "select Device, Time, id from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' and (LivelloPriorita = 'L1' or LivelloPriorita = 'L2')"
                    cursor.execute(query, (e[1],e[1], config.CORRELATION_MINUTES))
                    eventsAfter = cursor.fetchall()
                    for ea in eventsAfter:
                        if ea not in allSeenEvents: #We count only the unique events after x minutes
                            allSeenEvents.add(ea)
                            eventsAfterXminutes += 1
                devicesInterest.append((dev, eventsAfterXminutes))
        devicesInterest.sort(key = lambda tup: tup[1], reverse = True)
        fw2 = File_writer("CERN_devices_ranking")
        fw2.create_txt("../../res/")
        i = 0
        for di in devicesInterest:
            fw2.write_txt(di[0] + " - " + str(di[1]))
            print(di[0] + " - " + str(di[1]))
            i += 1
            if i == 80: break
                
                
                