'''
Created on 14 gen 2018

@author: Alessandro Corsair
'''
from datetime import datetime
from datetime import timedelta

class DeviceState(object):
    '''
    Represents a state of a device (activated, deactivated) in a certain time period.
    '''


    def __init__(self, timestamp, state = True):
        
        self.deviceState = state #True = activated
        self.timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        
    def getTimestamp(self):
        return self.timestamp    
        
        
class StateHandler(object):
    ''' 
    Creates not duplicated sequences. Can also create non overlapping sequences, or use an "overtime"
    to extend a sequence time lenght. Can also use an "undertime" to cut sequences too sparsely distributed
    over the time period.
    '''
    
    def __init__(self, undertime = 0, overtime = 0, overlaps = False):
        ''' Put overlaps = True if you you want to have overlapping sequences '''
        
        self.counter = None
        self.undertime = undertime
        self.overtime = overtime
        self.overlaps = overlaps
        self.devicesStates = dict()
        
    def addActivatedDevice(self, device, timestamp):
        ''' Adds a devices that has been found to be activated on a certain timestamp.
            Does nothing and return False if the event to be added is more then 5 minutes old, True otherwise.'''
        
        if self.minutesElapsed(5, timestamp): # 5 minutes are elapsed since the counter
            return False
        else:
            if device not in self.devicesStates: # the device is new...
                if len(self.devicesStates) == 0: # ...and it's the first to be added
                    newState = DeviceState(timestamp, True)
                    self.devicesStates[device] = newState
                    self.counter = newState.getTimestamp()
                else: #... and it's not the first 
                    newState = DeviceState(timestamp, True) 
                    self.devicesStates[device] = newState
                #if self.overlaps:
                
            else: #the device is not new
                if len(self.devicesStates) == 1:#there is only 1 device in the dictionary, 
                                                #and it is the same device that we are adding now
                    newState = DeviceState(timestamp, True)
                    self.counter = newState.getTimestamp() #reset the 5 minutes counter
                #else do nothing
        return True
    
    def minutesElapsed(self, mins, ts):
        ''' Returns True if "mins" minutes are elapsed since the last counter '''
        deltaMin = timedelta(minutes = mins)
        timestamp = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
        if self.counter == None:
            self.counter = timestamp
        if (timestamp - self.counter) > deltaMin:
            return True
        else:
            return False
    
    def getSequence(self):
        #ritorna i devices in devicesStates
        sequence = []
        for key in self.devicesStates:
            sequence.append(key)
        return sequence
    
    
    
    
    
    
    
    
    