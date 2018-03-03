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
    
    def __totalOccurrencesAnalysis(self, devicesExtra):
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
        self.occurrencesDB = self.__countEventDatabase(devicesExtra)
        
        # Count the relative occurrences (after n minutes) wrt to the reference device:
        self.noDupItemsetOccurrences = self.__relativeOccurrences(devicesExtra, moreThanOnce = True)
        
        # Compute the lift:
        lift = dict()
        for de in devicesExtra:
            if config.EXTRA:
                devExtra = de[0] + "--" + de[1]
            else:
                devExtra = de[0] + "--" 
            lift[devExtra] = round(self.noDupItemsetOccurrences[devExtra] / float(self.occurrencesDB[devExtra]), 2) * 100
                 
        #Save the data in the txt file:
        self.fw.write_txt("Analysis of the occurrences of the devices: " + str(devicesExtra[0] + devicesExtra[1]), True)
        self.fw.write_txt("DEVICE | PARTIAL | TOTAL | LIFT")
        rankings = []
        for de in devicesExtra:
            if config.EXTRA:
                devExtra = de[0] + "--" + de[1]
            else:
                devExtra = de[0] + "--" 
            couple = (devExtra, lift[devExtra])
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
    
    def __countEventDatabase(self, devicesExtra):
        '''
        Counts all the occurrences of the events of the given devices in the database. 
        Returns a dictionary (key = device--extra, value = occorrences).
        '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        occurrencesDB = dict() #key = device, value = occurrences
        if config.EXTRA:
            extraCond = " and state=%s"
        else:
            extraCond = ""
        for de in devicesExtra:
            if self.log:
                print("Processing device number " + str(devicesExtra.index(de) + 1) + "/" + str(len(devicesExtra)))
            # Find the total occurrences of the (network) devices in the DB:
            query = "select COUNT(*) from cern.electric where device= %s" +extraCond+ " and action='Alarm CAME'"
            if extraCond:
                cursor.execute(query, (de[0],de[1]))
            else:
                cursor.execute(query, (de[0],))
            events = cursor.fetchall()
            if config.EXTRA:
                devExtra = de[0] + "--" + de[1]
            else:
                devExtra = de[0] + "--"
            occurrencesDB[devExtra] = events[0][0]
        cursor.close()
        return occurrencesDB
    

    def __relativeOccurrences(self, devicesExtra, refDevice = None, priorityCond = True, moreThanOnce = False, correlationUniquness = None):
        '''
        Find the real itemset occurrences (with no duplicates) of the devices after n (config.CORRELATION_MINUTES) minutes.
        Basically, it is how many times each given device (in devicesExtra list) was found n minutes (config.CORRELATION_MINUTES) 
        after the reference device. If config.EXTRA is set, it will count occurrences with the proper state (tag, description or state)
        devicesExtra = (device, extra). The key of the returned dictionary is "device--state".
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
        # Create the entries in the dictionary for the device to analyze:
        noDupItemsetOccurrences = dict() # key = device, value = an object of the class "columnStats"
        for devExtra in devicesExtra:
            key = devExtra[0] + "--" + devExtra[1]
            noDupItemsetOccurrences[key] = 0
        if correlationUniquness == None:
            correlationUniquness = config.CORRELATION_UNIQUENESS
        if priorityCond: # Use the query with the priority -- MIGHT BE BROKEN
            print("THIS SHOULDN'T BE PRINTED (DatabaseNetworkCorrelator, __relativeOccurrences, priorityCond")
            query = ("select Device, Time, id from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
            cursor.execute(query, (refDevice, self.priority))
        elif isinstance(refDevice, tuple):
            #The reference device has the format (device,extra)
            if config.EXTRA:
                if config.EXTRA == "state":
                    extraCond = " and State=%s"
                elif config.EXTRA == "tag":
                    extraCond = " and Tag=%s"
                elif config.EXTRA == "description":
                    extraCond = " and Description=%s"
                query = ("select Device, Time, State, Tag, Description from electric where device=%s " + extraCond + " and action='Alarm CAME' order by time")
                cursor.execute(query, (refDevice[0],refDevice[1]))
            else:
                query = ("select Device, Time, State, Tag, Description from electric where device=%s and action='Alarm CAME' order by time")
                cursor.execute(query, (refDevice[0],))
        else: # ignore priority: select event from all priority levels
            query = ("select Device, Time, id from electric where device=%s and action='Alarm CAME' order by time")
            cursor.execute(query, (refDevice,) )
                        
        allSeenEvents = []
        events = cursor.fetchall()
        for e in events:
            strList = "%s" # The strList is used to select only the devices in the network
            for i in range(1, len(devicesExtra)):
                strList = strList + " OR device=" + "%s"
            # Query to find the events n minutes later
            query = ("select Device, Time, Id, State, Tag, Description from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' and (device=" 
                     + strList + ") order by time;")
            deviceNames = [x[0] for x in devicesExtra]
            tpl = (e[1], e[1], config.CORRELATION_MINUTES) + tuple(deviceNames)
            cursor.execute(query, tpl)
            eventsAfter = cursor.fetchall()
            if config.EXTRA == "state":
                extraIndex = 3
            elif config.EXTRA == "tag":
                extraIndex = 4
            elif config.EXTRA == "description":
                extraIndex = 5
            uniqueness = []
            # Now we count a device occurrence without duplicating events 
            for ea in eventsAfter:
                if ea not in allSeenEvents or moreThanOnce: #each event will be considered exactly once (unless moreThanOnce is True)
                    allSeenEvents.append(ea)
                    if config.EXTRA:
                        devExtra = ea[0] + "--" + ea[extraIndex]
                    else:
                        devExtra = ea[0] + "--"
                    if correlationUniquness: #if we want to count the event after only once per reference event
                        if devExtra not in uniqueness:
                            uniqueness.append(devExtra)
                            if config.EXTRA:
                                if (ea[0], ea[extraIndex]) in devicesExtra: #if the couple (device,extra) is one in which we are interested:
                                    noDupItemsetOccurrences[devExtra] = noDupItemsetOccurrences[devExtra] + 1
                            else:
                                if (ea[0], "") in devicesExtra:
                                    noDupItemsetOccurrences[devExtra] = noDupItemsetOccurrences[devExtra] + 1
                    else:
                        if config.EXTRA:
                            if (ea[0], ea[extraIndex]) in devicesExtra: #if the couple (device,extra) is one in which we are interested:
                                noDupItemsetOccurrences[devExtra] = noDupItemsetOccurrences[devExtra] + 1
                        else:
                            if (ea[0], "") in devicesExtra:
                                noDupItemsetOccurrences[devExtra] = noDupItemsetOccurrences[devExtra] + 1
        cursor.close()
        return noDupItemsetOccurrences
    
    
    def __findDoublePresence(self, dev1, dev2, devRef, priorRef):
        ''' 
        Finds how many times the devices dev1(+extra) and dev2(+extra) were found TOGETHER after the events of the reference
        device devRef with priority priorRef. Then it does the same without considering the priority.
        Returns a dictionary with the number of times the couples was found together in a given priority AT LEAST ONCE.
        Note: dev1 and dev2 are couples (device,extra)
        '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        query = ("select Device, Time, id, livellopriorita from electric where device=%s and action='Alarm CAME' order by time")
        cursor.execute(query, (devRef,) )
                        
        allSeenEvents = []
        events = cursor.fetchall()
        priorityOccurrences = dict() #key=priority, value=occurrences in that reference device priority 
        for key in ['L0', 'L1', 'L2', 'L3']:
            priorityOccurrences[key] = 0
        for e in events:
            strList = "%s OR device=" + "%s"
            # Query to find the events n minutes later
            query = ("select Device, Time, Id, State, Description, Tag from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' and (device=" 
                     + strList + ") order by time;")
            tpl = (e[1], e[1], config.CORRELATION_MINUTES, dev1[0], dev2[0])
            cursor.execute(query, tpl)
            eventsAfter = cursor.fetchall()
            if config.EXTRA == "state":
                extraIndex = 3
            elif config.EXTRA == "tag":
                extraIndex = 5
            elif config.EXTRA == "description":
                extraIndex = 4
            # Now we count a device occurrence without duplicating events
            priorityLevel = e[3]
            dev1check = False
            dev2check = False
            for ea in eventsAfter:
                if ea not in allSeenEvents: 
                    allSeenEvents.append(ea)
                    if config.EXTRA:
                        if (ea[0], ea[extraIndex]) == dev1:
                            dev1check = True
                        if (ea[0], ea[extraIndex]) == dev2:
                            dev2check = True
                    else:
                        if (ea[0], "") == dev1:
                            dev1check = True
                        if (ea[0], "") == dev2:
                            dev2check = True
            if dev1check and dev2check:
                priorityOccurrences[priorityLevel] += 1
        cursor.close()
        return priorityOccurrences, len(events)
    
    def __findSameTimestampOccurences(self, dev1, dev2):
        ''' Returns the number of times in which dev1(+extra) was found at the same timestamp of dev2(+extra) '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        if config.EXTRA:
            if config.EXTRA == "state":
                extraCond = " and State=%s"
            elif config.EXTRA == "tag":
                extraCond = " and Tag=%s"
            elif config.EXTRA == "description":
                extraCond = " and Description=%s"
            query = ("select Device, Time, id, State, Tag, Description from electric where action='Alarm CAME' and (device=%s)" + extraCond + " order by Time;")
            cursor.execute(query, (dev1[0],dev1[1]))
        else:
            query = ("select Device, Time, id, State, Tag, Description from electric where action='Alarm CAME' and (device=%s) order by Time;")
            cursor.execute(query, (dev1[0],))
        
        events = cursor.fetchall()
        sameTime = 0
        for e in events:
            if config.EXTRA:
                query = ("select count(*) from electric where time=(%s) and action='Alarm CAME' " + extraCond + " and device= %s;")
                cursor.execute(query, (e[1], dev2[1], dev2[0]))
            else:
                query = ("select count(*) from electric where time=(%s) and action='Alarm CAME' and device= %s;")
                cursor.execute(query, (e[1], dev2[0]))
            counts = cursor.fetchall()
            sameTime += counts[0][0]
        cursor.close()
        return sameTime
    
    def checkGeneralCorrelation(self):
        ''' Checks if the devices in the network are correlated (i.e. appear together) also in the rest of the DB '''
        # First, we must find the set of devices that are correlated by an high single (or double) activation
        self.fw.write_txt("GENERAL CORRELATIONS:", True)
        self.fw.write_txt("Node1 --> label1 | label2 --> Node2: AVG & ST.DEV. of Node2 w.r.t. Node1 (reference events)")
        self.fw.write_txt("")
        
        for el in self.edgeLabels:
            #Note: el[0] and el[1] have the format "device--extra"
            #Create the couples (device, extra) for each el[i]
            device = re.compile('(.*?)\-\-').findall(el[0])
            extra = ""
            if config.EXTRA:
                extra = re.compile('\-\-(.*?)\Z').findall(el[0])
                extra = extra[0]
            couple0 = (device[0], extra)
            device = re.compile('(.*?)\-\-').findall(el[1])
            extra = ""
            if config.EXTRA:
                extra = re.compile('\-\-(.*?)\Z').findall(el[1])
                extra = extra[0]
            couple1 = (device[0], extra)
            #couple0 and couple1 are tuples like (device, extra)
            
            # Total events in database (only Alarm Came):
            occurrencesDB = self.__countEventDatabase([couple0, couple1])
            if occurrencesDB[el[0]] >= 4000 or occurrencesDB[el[1]] >= 4000:
                self.fw.write_txt("Correlations between " + el[0] + "(" + str(occurrencesDB[el[0]])
                                  + " total occurreces) and " + el[1] + "(" + 
                                  str(occurrencesDB[el[1]]) + " total occurrences) not analyzed: there are too many events.")
                self.fw.write_txt("")
                continue
            # Average and standard deviation w.r.t. el[0] (ref.device):
            devicesDict0 = colAnal.find_column_distribution(couple0, ["L0", "L1", "L2", "L3"], [couple1])
            # Average and standard deviation w.r.t. el[1] (ref.device):
            devicesDict1 = colAnal.find_column_distribution(couple1, ["L0", "L1", "L2", "L3"], [couple0])
            # Relative events n minutes after el[0] (ref.device):
            noDupItemsetOccurrences0 = self.__relativeOccurrences([couple1], couple0, priorityCond = False, moreThanOnce = True)
            # Relative events n minutes after el[1] (ref.device):
            noDupItemsetOccurrences1 = self.__relativeOccurrences([couple0], couple1, priorityCond = False, moreThanOnce = True)
            # Relative events n minutes after el[0] (ref.device) WITHOUT UNIQUENESS:
            dupItemsetOccurrences0 = self.__relativeOccurrences([couple1], couple0, priorityCond = False, moreThanOnce = True, correlationUniquness = False)
            # Relative events n minutes after el[1] (ref.device) WITHOUT UNIQUENESS:
            dupItemsetOccurrences1 = self.__relativeOccurrences([couple0], couple1, priorityCond = False, moreThanOnce = True, correlationUniquness = False)
            # Relative events n minutes after el[0] (ref.device) WITHOUT UNIQUENESS AND WITHOUT DUPLICATES:
            realItemsetOccurrences0 = self.__relativeOccurrences([couple1], couple0, priorityCond = False, moreThanOnce = False, correlationUniquness = False)
            # Relative events n minutes after el[1] (ref.device) WITHOUT UNIQUENESS AND WITHOUT DUPLICATES:
            realItemsetOccurrences1 = self.__relativeOccurrences([couple0], couple1, priorityCond = False, moreThanOnce = False, correlationUniquness = False)
            # How many times the couples were found together after the reference event:
            couplesPriorityDict, totRefEvents = self.__findDoublePresence(couple0, couple1, 
                                                                                               self.referenceDevice, 
                                                                                               self.priority)
            # Find how many times dev1 and dev2 had the same timestamp:
            sameTimeOccurrences = self.__findSameTimestampOccurences(couple0, couple1)
            # Case in which no duplication 
            if not noDupItemsetOccurrences0 or not noDupItemsetOccurrences1:
                self.fw.write_txt("No correlations between this two devices.")
                self.fw.write_txt("")
                continue
            
            # SHOW CORRELATIONS IN TXT
            self.fw.write_txt(el[0] + "--> " + str(el[2]) + "|" + str(el[3]) + " --> " + el[1])
            self.fw.write_txt("------ " + el[0] + " and " + el[1] + " were found together (at least once) after: ")
            self.fw.write_txt("       " + str(couplesPriorityDict['L0']) + "/" + str(totRefEvents) + " events of " +self.referenceDevice
                              + " (priority L0)")
            self.fw.write_txt("       " + str(couplesPriorityDict['L1']) + "/" + str(totRefEvents) + " events of " +self.referenceDevice
                              + " (priority L1)")
            self.fw.write_txt("       " + str(couplesPriorityDict['L2']) + "/" + str(totRefEvents) + " events of " +self.referenceDevice
                              + " (priority L2)")
            self.fw.write_txt("       " + str(couplesPriorityDict['L3']) + "/" + str(totRefEvents) + " events of " +self.referenceDevice
                              + " (priority L3)")
            totPriors = couplesPriorityDict['L0'] + couplesPriorityDict['L1'] + couplesPriorityDict['L2'] + couplesPriorityDict['L3']
            self.fw.write_txt("       " + str(totPriors) + "/" + str(totRefEvents) + " events of " +self.referenceDevice
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
            self.fw.write_txt("The correlation between the 2 devices was found to be generally valid (without the reference device) - (AT LEAST ONCE)" + 
                  str(noDupItemsetOccurrences1[el[0]] + noDupItemsetOccurrences0[el[1]] - sameTimeOccurrences * 2 - totPriors * 2) + " times")
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
                
                
                