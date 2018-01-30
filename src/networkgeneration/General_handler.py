'''
Created on 03 gen 2018

@author: Alessandro Corsair
'''

from helpers.File_writer import File_writer
from networkgeneration.Log_extractor import Log_extractor
import copy

class General_handler(object):
    '''
    A unique class that can handle data from all the file-priority combinations
    '''

    def __init__(self):
        
        self.unique_devices = [] #a list with no duplicates
        self.device_locations = dict() # dictionary with key = device and value = tuples with locations H0, H1, H2
        
    def add_devices(self, device_list):
        ''' Adds all the unique devices to the self list '''
        for d in device_list:
            if d not in self.unique_devices:
                self.unique_devices.append(d)
                
    def save_to_file(self):
        ''' Saves the list of all the selected variables in a txt file '''
        fw = File_writer("Unique_devices", "List")
        fw.write_list(self.unique_devices)
        
    def getLocations(self):
        log_extractor = Log_extractor()
        devList = copy.deepcopy(self.unique_devices)
        self.device_locations = log_extractor.find_location(devList)
        
    def get_device_locations(self):
        return self.device_locations
                
    def get_unique_devices(self):
        return self.unique_devices
