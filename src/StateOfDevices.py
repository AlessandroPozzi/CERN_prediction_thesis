'''
Created on 14 gen 2018

@author: Alessandro Corsair
'''
from datetime import datetime
from datetime import timedelta
from File_writer import File_writer



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
    
    fwDebug = File_writer("last_execution_state_debug") #logging and debuggin purposes
    fwDebug.create_txt("../res/newres/")
    eventiRicevuti = 0
    eventiScartatiDuplicati = 0
    eventiOltre5Minuti = 0
    eventiScartatiSovrapposti = 0
    specialSingleSequences = 0
    
    def __init__(self, undertime = 0, overtime = 0, overlaps = False):
        ''' Put overlaps = True if you you want to have overlapping sequences '''
        
        self.counter = None
        self.undertime = undertime
        self.overtime = overtime
        self.overlaps = overlaps
        self.devicesStates = dict()
        self.singleDeviceReady = False # If True, it signals that there is a device ready to be saved as a single sequence [dev1]

        
    def addActivatedDevice(self, device, timestamp):
        ''' Adds a devices that has been found to be activated on a certain timestamp.
            Does nothing and return False if the event to be added is more then 5 minutes old, True otherwise.'''
        StateHandler.eventiRicevuti += 1
        
        if self.minutesElapsed(5, timestamp): # 5 minutes are elapsed since the counter
            StateHandler.eventiRicevuti -= 1
            StateHandler.eventiOltre5Minuti += 1
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
                    StateHandler.eventiScartatiDuplicati += 1
                    newState = DeviceState(timestamp, True)
                    self.counter = newState.getTimestamp() #reset the 5 minutes counter
                    self.singleDeviceReady = True
                    self.singleDevice = device
                #else do nothing
                else:
                    self.singleDeviceReady = True
                    self.singleDevice = device
                    StateHandler.eventiScartatiSovrapposti += 1
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
    
    def check_device_ready(self):
        if self.singleDeviceReady:
            self.singleDeviceReady = False
            return True
        else:
            return False
        
    def get_device_ready(self):
        dev = self.singleDevice
        StateHandler.specialSingleSequences += 1
        #self.get_device_ready = ""
        return [dev]
    
    def debugFile(self):
        StateHandler.fwDebug.write_txt("EVENTI RICEVUTI: " + str(StateHandler.eventiRicevuti))
        StateHandler.fwDebug.write_txt("EVENTI DUPLICATI SCARTATI: " + str(StateHandler.eventiScartatiDuplicati))
        StateHandler.fwDebug.write_txt("EVENTI FUORI DAI 5 MINUTI: " + str(StateHandler.eventiOltre5Minuti))
        print("--- StateHandler ---")
        print("EVENTI RICEVUTI: " + str(StateHandler.eventiRicevuti))
        print("EVENTI DUPLICATI SCARTATI tipo [dev1dev1dev1]: " + str(StateHandler.eventiScartatiDuplicati))
        print("EVENTI SOVRAPPOSTI SCARTATI tipo [dev1dev2dev1dev2]: " + str(StateHandler.eventiScartatiSovrapposti))
        print("SEQUENZE SINGOLE SPECIALI CREATE tipo [dev1] da [dev1dev1]: " + str(StateHandler.specialSingleSequences))
        print("EVENTI MESSI IN SEQUENZE: " + (str(StateHandler.eventiRicevuti - 
                                                  StateHandler.eventiScartatiDuplicati - 
                                                  StateHandler.eventiScartatiSovrapposti)))
        print("EVENTI FUORI DAI 5 MINUTI (#itemsets): " + str(StateHandler.eventiOltre5Minuti))
    
    
    
    
    
    