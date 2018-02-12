'''
Created on 24 gen 2018
@author: Alessandro Corsair

Note: The following are my database columns (+ index). The order of columns you have must be the same.
Time, H0, H1, H2, Device, Tag, Description, PrevValue, Value, Unit, State, Action, Type, AlarmPriority, Week, Device_FirstLetter, 
   0,  1,  2,  3,      4,    5,          6,         7,      8,   9,    10,     11,   12,            13,   14,                 15,           

Device_SecondLetter_LevelOfTension, Device_ThirdLetter_Type, Device_Forth_Number, Device_Fifth_LHCspsMeyrin, Device_Sixth_Position, id, LivelloPriorita
                                16,                      17,                  18,                        19,                    20, 21,              22
'''
from datetime import datetime
from datetime import timedelta
import numpy as np
#from helpers.DataError import DataError
from DataError import DataError
from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.preprocessing import StandardScaler
from comtypes.npsupport import numpy
from sklearn.cluster import MeanShift, estimate_bandwidth
import math

markedEvents = [] # a global list that contains all the events that have been already processed (i.e. "marked")

class EventState(object):
    '''
    Represents a single event, plus some data about its temporal position w.r.t. other events
    '''

    def __init__(self, event, previous_event, initTime):
        self.rawEvent = event # the event taken from the DB (i.e. a tuple)
        self.timestamp = datetime.strptime(event[0], '%Y-%m-%d %H:%M:%S.%f')
        self.device = event[4]
        if previous_event == None: #i.e. this is the case of the first event
            self.timeDelta = None #time elapsed since PREVIOUS event
            self.timeDelta_ms = None
        else:
            previousTimestamp = datetime.strptime(previous_event[0], '%Y-%m-%d %H:%M:%S.%f')
            self.timeDelta = self.timestamp - previousTimestamp #time elapsed since PREVIOUS event
            self.timeDelta_ms = self.timeDelta.total_seconds() * 1000 + self.timeDelta.microseconds / 1000
        timeElapsed = (self.timestamp - datetime.strptime(initTime, '%Y-%m-%d %H:%M:%S.%f')) #time elapsed since REFERENCE event
        seconds = timeElapsed.total_seconds()
        milliseconds = timeElapsed.microseconds / 1000
        self.millisecondsElapsed = seconds * 1000 + milliseconds #millseconds elapsed since REFERENCE event
        
    def getEvent(self):
        return self.rawEvent
    
    def getDevice(self):
        return self.device
    
    def getTimeDelta(self):
        return self.timeDelta
    
    def getTimestamp(self):
        return self.timestamp
    
    def getMilliSecondsElapsed(self):
        return self.millisecondsElapsed


class ClusterHandler(object):
    ''' 
    Stores events in a 5 minutes limit and detects clusters in them.
    This object can deal with only a single sequence, not more 
    '''
    
    def __init__(self, startingEvent):
        self.eventStateList = [] # the list of events (EventState objects) to be considered for the clustering
        self.clustersList = [] # the list of clusters. Each cluster is a list of "EventState" objects
        self.averageDelta = float(0)
        self.startingEvent = startingEvent # the event that starts the 5 minutes analysis
        self.referenceEvent = None
        
    def addEvent(self, event):
        ''' 
        Adds this event to the list to be considered for the clustering.
        If the event has been already considered in another cluster, it is not added.
        '''
        global markedEvents
        if event in markedEvents:
            return
        else:
            markedEvents.append(event)
            if self.referenceEvent == None:
                eventState = EventState(event, None, self.startingEvent[0])
            else:
                eventState = EventState(event, self.referenceEvent, self.startingEvent[0])
            self.eventStateList.append(eventState) # update the list of events (states)
            self.referenceEvent = event # the current event becomes the next "previous event"
            
    def findClustersOfflineAverage(self, fw, debug = False):
        '''
        Tries to find the clusters in the previously stored events using the "offline average" technique,
        i.e. it computes the average of the temporal difference between each event and then
        separates groups of events that have a temporal difference higher than the average.
        NOTA: questo metodo non guarda il distacco temporale tra l'evento di riferimento e il primo evento incontrato.
        '''
        if self.eventStateList == []:
            raise DataError("No events in this sequence")
        else:
            if debug:
                fw.write_txt("STARTING EVENT: " + self.startingEvent[0] + " - " + self.startingEvent[4], newline=True)
            esl = self.eventStateList
            if len(esl) == 1: # Single event in 5 minutes, i.e. no need to do clustering
                if debug:
                    fw.write_txt("Single event " + str(esl[0].getTimestamp()) + " - " + esl[0].getDevice())
                self.clustersList.append([esl[0]])
                return
            # Here the real clustering starts.
            # First, get all the temporal differences between consecutive events and compute SUM and AVERAGE
            timeSum = timedelta(seconds = 0)
            for es in esl:
                delta = es.getTimeDelta()
                if delta != None: #delta is equal to None for the first event in 5 minutes
                    timeSum += delta
            average = timeSum / (len(esl)-1) 
            if debug:
                fw.write_txt("AVERAGE = " + str(average))
            # Now, divide the groups of events based on the average:
            j = 0 # "j" indicates the first element from which we'll start the next cluster
            for i in range(1, len(esl)):
                if esl[i].getTimeDelta() > 1 * average:
                    newCluster = esl[j:i]
                    self.clustersList.append(newCluster)
                    if debug:
                        fw.write_txt("CLUSTER:")
                        for s in newCluster:
                            fw.write_txt(str(s.getTimestamp()) + " - " + s.getDevice())
                    j = i
            l = len(esl) - 1 # "l" = total number of events in this 5 minutes
            # Add the last cluster (if left):
            if l <> j:
                newCluster = esl[j:l+1]
                self.clustersList.append(newCluster)
                if debug:
                    fw.write_txt("CLUSTER:")
                    for s in newCluster:
                        fw.write_txt(str(s.getTimestamp()) + " - " + s.getDevice())
                        
    def findClustersAverageDeviation(self, fw, debug = False):
        '''
        Tries to find the clusters in the previously stored events using the "average deviation" technique,
        i.e. it computes the average and the standard deviation of the temporal difference between each event and then
        separates groups of events that have a temporal difference higher than the average + standard deviation
        NOTA: questo metodo non guarda il distacco temporale tra l'evento di riferimento e il primo evento incontrato.
        '''
        if self.eventStateList == []:
            raise DataError("No events in this sequence")
        else:
            if debug:
                fw.write_txt("STARTING EVENT: " + self.startingEvent[0] + " - " + self.startingEvent[4], newline=True)
            esl = self.eventStateList
            if len(esl) == 1: # Single event in 5 minutes, i.e. no need to do clustering
                if debug:
                    fw.write_txt("Single event " + str(esl[0].getTimestamp()) + " - " + esl[0].getDevice())
                self.clustersList.append([esl[0]])
                return
            # Now the real clustering starts.
            # First, get all the temporal differences between consecutive events and compute SUM, AVERAGE and ST.DEV.

            # SUM and AVERAGE:
            timeSum = 0
            for es in esl:
                delta = es.timeDelta_ms
                if delta != None: #delta is equal to None for the first event in 5 minutes
                    timeSum += delta
            average_ms = timeSum / (len(esl)-1) 

            # STANDARD DEVIATION:
            totalNumerator = 0 # SUM_i{(x_i - average)^2}
            for es in esl:
                delta = es.timeDelta_ms
                if delta != None: #delta is equal to None for the first event in 5 minutes
                    gap = delta - average_ms # (x_i - average)
                    totalNumerator = gap * gap + totalNumerator
            stDev_ms = math.sqrt(totalNumerator / (len(esl)-1)) #standard deviation in milliseconds
            
            # Now, divide the groups of events based on the average + standard deviation:
            j = 0 # "j" indicates the first element from which we'll start the next cluster
            for i in range(1, len(esl)):
                if esl[i].timeDelta_ms > average_ms + stDev_ms:
                    newCluster = esl[j:i]
                    self.clustersList.append(newCluster)
                    j = i
            l = len(esl) # "l" = total number of events in this 5 minutes
            
            # Add the last cluster (if left):
            if l <> j:
                newCluster = esl[j:l]
                self.clustersList.append(newCluster)
            
            # Print "debug" info in the txt file:    
            if debug:
                # Show average and standard deviation
                fw.write_txt("AVERAGE = " + self.roundToStr(average_ms / 1000) + " seconds")
                fw.write_txt("STANDARD DEVIATION = " + self.roundToStr(stDev_ms / 1000) + " seconds")
                fw.write_txt("AVERAGE + ST.DEV. = " + self.roundToStr((average_ms + stDev_ms) / 1000) + " seconds")
                # Show the clusters 
                for cluster in self.clustersList:
                    fw.write_txt("CLUSTER:")
                    for s in cluster:
                        addition = ""
                        if s.timeDelta_ms != None:
                            addition = " (+" + self.roundToStr(s.timeDelta_ms / 1000) + "s)"
                        fw.write_txt(str(s.getTimestamp()) + " - " + s.getDevice() + addition)
                        
                        
    def findClustersStaticDistance(self, fw, timeDelta, debug = False):
        '''
        Finds the clusters in the previously stored events by separating groups that have a temporal
        distance one from the other that is at least higher than the given "timeDelta" value.
        '''
        if self.eventStateList == []:
            raise DataError("No events in this sequence")
        else:
            if debug:
                fw.write_txt("STARTING EVENT: " + self.startingEvent[0] + " - " + self.startingEvent[4], newline=True)
            esl = self.eventStateList
            if len(esl) == 1: # Single event in 5 minutes, i.e. no need to do clustering
                if debug:
                    fw.write_txt("Single event " + str(esl[0].getTimestamp()) + " - " + esl[0].getDevice())
                self.clustersList.append([esl[0]])
                return
            # now the real clustering begins:
            j = 0 # "j" indicates the first element from which we'll start the next cluster
            for i in range(1, len(esl)):
                if esl[i].getTimeDelta() > timeDelta:
                    newCluster = esl[j:i]
                    self.clustersList.append(newCluster)
                    if debug:
                        fw.write_txt("CLUSTER:")
                        for s in newCluster:
                            fw.write_txt(str(s.getTimestamp()) + " - " + s.getDevice())
                    j = i
            l = len(esl) - 1 # "l" = total number of events in this 5 minutes
            # Add the last cluster (if left):
            if l <> j:
                newCluster = esl[j:l+1]
                self.clustersList.append(newCluster)
                if debug:
                    fw.write_txt("CLUSTER:")
                    for s in newCluster:
                        fw.write_txt(str(s.getTimestamp()) + " - " + s.getDevice())
            
    def findClustersOnlineAverage(self):
        '''
        Tries to find the clusters in the previously stored events using the "online average" technique,
        i.e. it updates the average of the temporal differences between events while they are added one 
        by one, and separates groups of events with the delta greater than the average (with a margin).
        '''
        
        print("NOT DONE YET")
        
    def findClustersDBSCAN(self, fw, debug = False):
        data = self.buildDistanceMatrix()
        if data.shape[1] == 0: #if the data is not a square matrix (i.e. there are no events)
            return
        else:
            eps = 12000 # neighbor distance (in milliseconds) 
            min_samples = 3 #CORE SAMPLE INCLUDED! i.e. if min_samples=2 THEN the minimun dimension of the cluster is 2
            db = DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed").fit(data)
            core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
            core_samples_mask[db.core_sample_indices_] = True
            labels = db.labels_
            n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
            
            if debug:
                fw.write_txt("STARTING EVENT: " + self.startingEvent[0] + " - " + self.startingEvent[4], newline=True)
                fw.write_txt("Number of events: " + str(self.eventStateList.__len__()))
                fw.write_txt('Estimated number of clusters: %d' % n_clusters_)
                for i in range(len(self.eventStateList)):
                    event = self.eventStateList
                    fw.write_txt(str(event[i].getTimestamp()) + " - " + event[i].getDevice() + " --> " + str(labels[i]))
                fw.write_txt('Labelling of clusters: ' + str(labels))
        
            #Saving the clusters in the list:
            cluster = []
            cluster.append(self.eventStateList[0])
            previousLabel = labels[0]
            for i in range(1, len(self.eventStateList)):
                if labels[i] == previousLabel:
                    #the event is in the same cluster
                    cluster.append(self.eventStateList[i])
                else:
                    #the event is in another cluster
                    self.clustersList.append(cluster)
                    cluster = []
                    cluster.append(self.eventStateList[i])
                    previousLabel = labels[i]
    
    def findClustersMeanShift(self, fw, debug=False):
        featureArray = self.buildFeatureArray()
        nsamples = featureArray.shape[0]
        if nsamples <= 1:
            return
        bandwidth = estimate_bandwidth(featureArray, n_samples=nsamples)
        ms = MeanShift(bandwidth=bandwidth, bin_seeding=True)
        ms.fit(featureArray)
        labels = ms.labels_
        cluster_centers = ms.cluster_centers_
        labels_unique = np.unique(labels)
        n_clusters_ = len(labels_unique)
        if debug:
            fw.write_txt("STARTING EVENT: " + self.startingEvent[0] + " - " + self.startingEvent[4], newline=True)
            fw.write_txt("Number of events: " + str(self.eventStateList.__len__()))
            fw.write_txt('Estimated number of clusters: %d' % n_clusters_)
            for i in range(0, len(self.eventStateList)):
                events = self.eventStateList
                fw.write_txt(str(events[i].getTimestamp()) + " - " + events[i].getDevice() + " --> " + str(labels[i]))
            fw.write_txt('Labelling of clusters: ' + str(labels))
        
    
    def buildDistanceMatrix(self):
        # Build the matrix of distances between events:
        matrix = []
        for event1 in self.eventStateList:
            matrixLine = []
            for event2 in self.eventStateList:
                cell = abs(event1.getMilliSecondsElapsed() - event2.getMilliSecondsElapsed())
                matrixLine.append(cell)
            matrix.append(matrixLine)
        # Convert it to a numpy object:
        numpyMatrix = np.matrix(matrix)
        return numpyMatrix
    
    def buildFeatureArray(self):
        # build an array with shape=[n_samples, n_features]. In this case we have only 1 feature (temporal distance)
        featureArray = [] # a list for each sample, containing the value of the only feature we have
        for event in self.eventStateList:
            sampleLine = [event.getMilliSecondsElapsed()]
            featureArray.append(sampleLine)
        return np.array(featureArray)
    
    def roundToStr(self, x):
        ''' Rounds to 2 significant digits the passed number. Return a string of the number '''
        return str(round(x,2))
    
    def getClusters(self):
        '''
        Returns the clusters in a list of lists. 
        Each cluster is a list of "EventState" objects.
        '''
        return self.clustersList
    
    
    