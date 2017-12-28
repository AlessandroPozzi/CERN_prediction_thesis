'''
Created on 15 nov 2017
@author: Alessandro Pozzi, Lorenzo Costantini
'''

import re
import pandas as pd

class Data_extractor:
    '''
    Manages the extraction and processing of data in the CERN txt files, along with some data statistic features.
    FORMAT of the txt files:
    -------------------------------------------------------------------------------------------------------------
    DEVICE device_name: 
        PRIORITY level:
        
            Distinct devices after 5 minutes: [
                    ['device_name', 'device_name', 'device_name', ...], 
                    ...
                    [list of devices],
                    [list of devices],
            ]
        
            ===> FREQUENT ITEMSETS in Distinct devices after 5 minutes: (with support=X the threshold is Y):
            frozenset([u'device_name']) :  x1
            frozenset([u'device_name']) :  x2
                              
        PRIORITY level:
            ...
            ...
    -------------------------------------------------------------------------------------------------------------

    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.priority = ('L0', 'L1', 'L2', 'L3')
        self.variable_names = [] #to be used as nodes in the network
        self.true_file_names = [] #stores the names of the files
        self.txt_file_names = []
        self.priority_selected = ""
        self.events_by_file = dict() #a dictionary that has filenames as keys, and a list of tuples as values. Each tuple is a couple ([device list], priority)
        self.ranked_devices = [] #list of tuples: (device, frequency, occurrences)
        self.skipped_lines = 0
        self.totalRows = 0

    def extract(self, txtfile, true_device_name, select_priority):
        '''
        Extracts data from the txt file
        
        Parameters
        ----------
        txtfile: the name of the txt file found in the /res folder
        true_device_name : the real name of the device in the events
        select_priority : a string that contain the priority level (L0,L1,L2 or L3) to be considered
        '''
        self.true_file_names.append(true_device_name)
        self.txt_file_names.append(txtfile)
        self.priority_selected = select_priority
        self.events_by_file[true_device_name] = []
        
        with open ('../res/' + txtfile +'.txt', 'r') as in_file:
            all_events = 1
            p = 0
            
            for line in in_file.readlines():
                #NOTE: the order of IF conditions IS RELEVANT
                
                if all_events==1 and self.priority[p-1] == select_priority: # Here we are in "Distinct devices after 5 min"
                    self.store_line(line, true_device_name, self.priority[p-1])
                
                if re.search(r"PRIORITY", line): # Here we are in the "PRIORITY level" line
                    p+=1 # priority level increase
                    all_events = 1
                
                        
                if re.search(r"==>", line): #here we are in the FREQUENT ITEMSETS
                    all_events = 0
            

    def store_line(self, line, file_name, p):
        ''' 
        Stores the devices in the given line (which should be found in the "Distinct devices after 5 minutes" set for later use 
        and also stores the priority
        '''
        devices_found = self.find_devices(line)
        if devices_found:
            tupl = (devices_found, p)
            self.events_by_file[file_name].append(tupl)
            self.totalRows = self.totalRows + 1
            
    def find_devices(self, line):
        ''' Finds all the devices ID in the given line '''
        findings = re.compile('\'\S+\'').findall(line)
        processed_findings = []
        for f in findings:
            processed_findings.append(f.replace("'", ""))
        return processed_findings
    
    def prepare_candidates(self):
        '''
        Extracts occurrences and frequency of the devices (i.e. the candidate for becoming variables of the network).
        This data is stored in ranked_devices, as a tuple of the kind (device, frequency, occurrences).
        '''
        self.ranked_devices = []
        frequency_by_device = dict() # key = device, value = sum of frequencies of device

        for key in self.events_by_file:
            occurrences = dict() #key = device; value = number of occurrences in SINGLE FILE
            total_events = len(self.events_by_file[key])
            for tupl in self.events_by_file[key]: # each tuple is: (device_list, priority)
                for d in tupl[0]: 
                    if d not in occurrences: #create new key
                        occurrences[d] = 1
                    else: #update key
                        occurrences[d] = occurrences[d] + 1
            for d in occurrences:
                frequency_by_device[d] = round( occurrences[d] / float(total_events) , 2)
        
        for d in frequency_by_device:
            tupl = (d, frequency_by_device[d], occurrences[d])
            self.ranked_devices.append(tupl)
            
        
    def select_candidates(self, var_type, support, MIN, MAX):
        
        if var_type == "occurrences":
            self.ranked_devices.sort(key = lambda tup: tup[2], reverse=True)
        elif var_type == "frequency":
            self.ranked_devices.sort(key = lambda tup: tup[1], reverse=True)
            
        ordered_ranking = [i for i in self.ranked_devices if i[0] != self.true_file_names[0]] # helper list with no file device in it
            
        for i in range(len(ordered_ranking)):
            NUM = len(self.variable_names)
            device = ordered_ranking[i][0]
            frequency = ordered_ranking[i][1]
            
            if NUM < MIN:
                self.variable_names.append(device)
            elif NUM < MAX:
                if var_type == "frequency":
                    if frequency > support:
                        self.variable_names.append(device)
                elif var_type == "occurrences":
                    self.variable_names.append(device)
            else:
                break
    
    
    def build_dataframe(self, training_instances="none"):
        ''' 
        Builds and returns the pandas dataframe
        training_instances = 
            all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes") 
            all_events_priority    -- like all_event but instead of using [0, 1] as values for variables, uses the priority related to the event: [0, L0, L1, L2, L3]
        priority_node      =  True if you want to ignore the priority node
        '''
        dict_data = dict()
              
        for key in self.events_by_file:
            for tupl in self.events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                if self.not_empty_check(tupl[0]): #i.e. consider only events lines that generate a NON-EMPTY training instance
                    for ud in self.variable_names:
                        if ud in tupl[0] or ud==key:
                            if training_instances == "all_events_priority":
                                value = tupl[1]
                            else:
                                value = 1
                        else:    
                            value = 0
                            
                        if ud in dict_data: #key already present
                            dict_data[ud].append(value)
                        else: #create new key in dictionary
                            dict_data[ud] = []
                            dict_data[ud].append(value)
        for ud in self.variable_names:
            dict_data[ud].append(0)

        data = pd.DataFrame(dict_data)
        data.to_csv(path_or_buf="../output/dataframes/" + self.txt_file_names[0] + "_" + 
                    self.priority_selected[0] + "_" + "dataframe.csv")
        return data

        
    def not_empty_check(self, device_list):
        ''' Takes a list of devices and checks if at least one variable is present in that list.
        If the priority node is not present, will required that at least 2 devices are present
        '''

        i = 0
        for d in self.variable_names:
            if d in device_list:
                i += 1
                if i >=1: #i>=x : accepts training instances with at least x "1".
                    return True
        self.skipped_lines += 1
        return False
    
    def get_data_txt(self):
        return self.data_txt
    
    def get_skipped_lines(self):
        return self.skipped_lines
            
    def nodata(self):
        ''' 
        Checks if there is some data in this priority - file combination. 
        '''
        fl = self.true_file_names[0]
        if self.events_by_file[fl] == []:
            return True
        for row in self.events_by_file[fl]:
            for device in row:
                if device != fl:
                    return False
        return True
                
    def get_variable_names(self):
        return self.variable_names  
    
    def get_ranked_devices(self):
        return self.ranked_devices
    
    def add_variable_names(self, names):
        for n in names:   
            self.variable_names.append(n)
                