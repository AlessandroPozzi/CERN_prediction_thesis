'''
Created on 14 feb 2018

@author: Alessandro Pozzi
'''
import mysql.connector
from File_writer import File_writer
import config
import columnAnalyzer as colAnal

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
        ---- Parameters ----
        
        '''
        self.log = log
        self.post = False
        self.referenceDevice = refDevice
        self.priority = priority
        self.noDupItemsetOccurrences = dict()
        self.fw = File_writer(self.referenceDevice, self.priority, "preProcessingAnalysis")
        self.fw.create_txt("../../output/preProcessingAnalysis/")
        self.fw.write_txt("File of device " + self.referenceDevice + " with priority " + self.priority + " (PRE network creation)")
    
    def totalOccurrencesAnalysis(self, devices):
        ''' 
        This should not be called directly. Use totalOccurrencesCandidatesAnalysis
        or totalOccurrencesNetworkAnalysis methods instead. 
        '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        # Count the total appearances of the devices in the database
        self.occurrencesDB = dict() #key = device, value = occurrences
        for d in devices:
            if self.log:
                print("Processing device number " + str(devices.index(d) + 1) + "/" + str(len(devices)))
            # Find the total occurrences of the (network) devices in the DB:
            query = "select COUNT(*) from cern.electric where device= %s and action='Alarm CAME'"
            cursor.execute(query, (d,))
            events = cursor.fetchall()
            self.occurrencesDB[d] = events[0][0]
            
        # Find the real itemset occurrences (with no duplicates) of the (network) devices
        allSeenEvents = []
        query = ("select Time, id from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
        cursor.execute(query, (self.referenceDevice, self.priority))
        events = cursor.fetchall()
        for e in events:
            strList = "%s" # The strList is used to select only the devices in the network
            for i in range(1, len(devices)):
                strList = strList + " OR device=" + "%s"
            # Query to find the events n minutes later
            query = ("select Device, Time, Id from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' and (device=" 
                     + strList + ") order by time;")
            tpl = (e[0], e[0], config.CORRELATION_MINUTES) + tuple(devices)
            cursor.execute(query, tpl)
            eventsAfter = cursor.fetchall()
            uniqueness = []
            # Now we count a device occurrence without duplicating events 
            for ea in eventsAfter:
                if ea not in allSeenEvents: #each event will be considered exactly once
                    allSeenEvents.append(ea)
                    if config.CORRELATION_UNIQUENESS:
                        if ea[0] not in uniqueness:
                            uniqueness.append(ea[0])
                            if ea[0] not in self.noDupItemsetOccurrences: #key already present
                                self.noDupItemsetOccurrences[ea[0]] = 1
                            else:
                                self.noDupItemsetOccurrences[ea[0]] = self.noDupItemsetOccurrences[ea[0]] + 1
                    else:
                        if ea[0] not in self.noDupItemsetOccurrences: #key already present
                            self.noDupItemsetOccurrences[ea[0]] = 1
                        else:
                            self.noDupItemsetOccurrences[ea[0]] = self.noDupItemsetOccurrences[ea[0]] + 1
                        

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
            #self.fw.write_txt("DEVICE: " + d)
            #self.fw.write_txt("Itemset occurrences (possibly with dup.): " + str(self.itemsetOccurrences[d]))
            #self.fw.write_txt("Itemset occurrences (without duplicates): " + str(self.noDupItemsetOccurrences[d]))
            #self.fw.write_txt("Database occurrences (total, no duplic.): " + str(self.occurrencesDB[d]))
            #self.fw.write_txt("In the log, " + d + " total activations happen " + str(lft) + "% of the times when also " + 
            #                  self.referenceDevice + " appears.")
            
        #If this is object is being used to find the candidates nodes of the network, return the lift:
        if self.post == False:
            return lift
        
    def totalOccurrencesNetworkAnalysis(self):
        self.totalOccurrencesAnalysis(self.devices)
    
    def totalOccurrencesCandidatesAnalysis(self, candidates = []):
        if not candidates:
            for tpl in self.rankedDevices:
                candidates.append(tpl[0])
        return self.totalOccurrencesAnalysis(candidates)
    
    def checkGeneralCorrelation(self):
        ''' Checks if the devices in the network are correlated (i.e. appear together) also in the rest of the DB '''
        # First, we must find the set of devices that are correlated by an high single (or double) activation
        self.fw.write_txt("GENERAL CORRELATIONS:", True)
        self.fw.write_txt("Node1 --> label1 | label2 --> Node2: AVG & ST.DEV. of Node2 w.r.t. Node1 (reference events)")
        for d in self.devices:
            correlationSet = set()
            for el in self.edgeLabels:
                if (el[0]==d) and (el[2] >= 0.70):
                    correlationSet.add(el[0])
                    correlationSet.add(el[1])
                    # Check if this correlation also exists in general in the DB:
                    if len(correlationSet) != 0:
                        devicesDict = colAnal.find_column_distribution(el[0], ["L0", "L1", "L2", "L3"], el[1])
                        # SHOW CORRELATIONS IN TXT
                        self.fw.write_txt(el[0] + "--> " + str(el[2]) + "|" + str(el[3]) + " --> " + el[1] + ": " + 
                                            str(round(devicesDict[el[1]].msAverage,2)) + " " + 
                                            str(round(devicesDict[el[1]].msStandDev,2)))
                        
            
        
        
        