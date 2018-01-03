'''
Created on 03 gen 2018

@author: Alessandro Corsair
'''

from File_writer import File_writer

class General_handler(object):
    '''
    classdocs
    '''


    def __init__(self):
        
        self.unique_devices = [] #a list with no duplicates
        
    def add_devices(self, device_list):
        ''' Adds all the unique devices to the self list '''
        for d in device_list:
            if d not in self.unique_devices:
                self.unique_devices.append(d)
                
    def save_to_file(self):
        fw = File_writer("Unique_devices", "List")
        fw.write_list(self.unique_devices)
                
    def get_unique_devices(self):
        return self.unique_devices
