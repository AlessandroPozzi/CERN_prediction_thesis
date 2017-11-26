'''
Created on 15 nov 2017

@author: Alessandro Corsair
'''

import re
import pandas as pd
import numpy as np

priority = ('L0', 'L1', 'L2', 'L3')
data_txt = [] #This list will contain "data_p" elements, one per txt file
# data_p = [[], [], [], []] this list contains 4 elements, one for each priority. 
#Each element is a list of tuples of the type: (priority, devices, support). The devices are a list of frequent devices with that particular priority and support
variable_names = [] #to be used as nodes in the network
file_names = [] #stores the names of the files
events_by_file = dict() #a dictionary that has filenames as keys, and a list of tuples as values. Each tuple is a couple ([device list], priority)
ranked_devices_by_count = [] #list of tuples that show which devices appear more
unique_frequent_devices_by_file = dict() #
ranked_device_by_frequency = [] #list of tuples that show which devices appear more, considering individual frequencies

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

    def extract(self, txtfile, true_device_name, ignore_priority = []):
        '''
        Extracts data from the txt file
        
        Parameters
        ----------
        txtfile: the name of the txt file found in the /res folder
        true_device_name : the real name of the device in the events
        ignore_priority : a list that contain the priority levels (L0,L1,L2,L3) to be completely ignored
        '''
        global unique_frequent_devices_by_file
        global events_by_file
        global file_names
        data_p = [[], [], [], []]
        file_names.append(true_device_name)
        events_by_file[true_device_name] = []
        unique_frequent_devices_by_file[true_device_name] = []
        
        with open ('../res/' + txtfile +'.txt', 'r') as in_file:
            all_events = 1
            p = 0
            min_support = 0
            
            for line in in_file.readlines():
                #NOTE: the order of IF conditions IS RELEVANT
                
                if all_events==1 and priority[p-1] not in ignore_priority:
                    self.store_line(line, true_device_name, priority[p-1])
                
                if re.search(r"PRIORITY", line):
                    p+=1 #priority level
                    all_events = 1
                
                if all_events==0 and priority[p-1] not in ignore_priority:
                    match = self.find_devices(line)
                    support = self.find_support(line)
                    if match and support:
                        content = (priority[p-1], match, support[0], min_support)
                        data_p[p-1].append(content)
                        self.unique_frequent_devices(match, true_device_name)
                        
                if re.search(r"==>", line):
                    all_events = 0 #i.e. we'll now see the processed frequent itemsets
                    min_support = self.find_min_support(line)
        
        data_txt.append(data_p)
            
    def find_devices(self, line):
        ''' Finds all the devices ID in the given line '''
        findings = re.compile('\'\S+\'').findall(line)
        processed_findings = []
        for f in findings:
            processed_findings.append(f.replace("'", ""))
        return processed_findings
    
    def find_support(self, line):
        ''' Finds the support value in the given line '''
        support = line.partition(":")[2]
        return re.compile('\d+').findall(support)
    
    def find_min_support(self, line):
        ''' Finds the lowest value that the "support" can assume. This is also called "threshold" in the txt file ''' 
        min_support = line.partition("threshold is ")[2]
        return int(re.compile('\d+').findall(min_support)[0])
    
    def unique_frequent_devices(self, devices, file_name):
        ''' Updates a list that contains the IDs of all the unique devices met until this point - to be used as variables in the BN '''
        global variable_names
        global unique_frequent_devices_by_file
        for dev in devices:
            if dev not in variable_names:
                variable_names.append(dev)
            if dev not in unique_frequent_devices_by_file[file_name]:
                unique_frequent_devices_by_file[file_name].append(dev)
                

    def store_line(self, line, file_name, p):
        ''' 
        Stores the devices in the given line (which should be found in the "Distinct devices after 5 minutes" set for later use 
        and also stores the priority
        '''
        global events_by_file
        devices_found = self.find_devices(line)
        if devices_found:
            tupl = (devices_found, p)
            events_by_file[file_name].append(tupl)
            
    def count_occurrences_all_devices(self):
        ''' Finds all the unique devices in all the events '''
        global events_by_file
        global ranked_devices_by_count #A list of tuples (device, occurrences)
        ranked_devices_by_count = []
        device_occurrences = dict() #this will help storing the # of occurrences
        
        for key in events_by_file:
            for line in events_by_file[key]:
                for d in line[0]: #[0] because it's a tuple
                    if d not in priority: #we don't want to count the priority
                        if d in device_occurrences:
                            device_occurrences[d] = device_occurrences[d] + 1 #update key
                        else:
                            device_occurrences[d] = 1 #create key
        
        for key in device_occurrences:
            ranked_devices_by_count.append((key, device_occurrences[key]))
        
        ranked_devices_by_count.sort(key = lambda tup: tup[1], reverse=True)
        return ranked_devices_by_count 
        
        
    def count_occurrences_variables(self):
        ''' Counts the occurrences of the variable names in all the "Distinct devices after 5 minutes" set '''
        global variable_names
        global events_by_file
        global ranked_devices_by_count #A list of tuples (device, occurrences)
        device_occurrences = dict() #this will help storing the # of occurrences
        
        for d in variable_names:
            device_occurrences[d] = 0
        
        for key in events_by_file:
            for tupl in events_by_file[key]:
                for d in tupl[0]:
                    if d not in priority: #CHECK THIS, COULD BE USELESS NOW
                        if d in variable_names:
                            device_occurrences[d] = device_occurrences[d] + 1
                    
        for key in device_occurrences:
            ranked_devices_by_count.append((key, device_occurrences[key]))
        
        ranked_devices_by_count.sort(key = lambda tup: tup[1], reverse=True)
        return ranked_devices_by_count 
    
    def frequency_occurences_variables(self):
        ''' NON FINITA '''
        global events_by_file
        global ranked 
        global ranked_device_by_frequency
        frequency_by_device = dict() #key = device; value = number of occurrences in SINGLE FILE
        
        for key in events_by_file:
            total_events = len(events_by_file[key])
            for tupl in events_by_file[key]:
                for d in tupl[0]:
                    if d not in frequency_by_device: #create new key
                        frequency_by_device[d] = 0
                    else: #update key
                        frequency_by_device[d] = frequency_by_device[d] + 1
            for d in frequency_by_device:
                frequency_by_device[d] = frequency_by_device[d] / total_events
            
                
        
    
    def take_n_variables(self, n):
        ''' choses the n most frequent devices and uses them as new variables '''
        global variable_names
        variable_names = []
        for i in range(0, n):
            variable_names.append(ranked_devices_by_count[i][0])
                
    
    def reset_variable_names(self, new_list):
        ''' Replaces the unique devices with a given list '''
        global variable_names 
        variable_names = new_list
    
    def build_dataframe(self, training_instances="none", priority_node = False):
        ''' 
        Builds and returns the pandas dataframe
        training_instances = 
            support                -- to use duplicated training instances based on the support of the frequent sets
            all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes")
            all_events_with_causes -- like all_events, but also considers the 6 causes variables
            all_events_priority    -- like all_event but instead of using [0, 1] as values for variables, uses the priority related to the event: [0, L0, L1, L2, L3]
        priority_node      =  True if you want to ignore the priority node
        '''
        dict_data = dict()
        if priority_node:
            print("Priority node will be now added.")    
            dict_data['priority'] = [] #add the "priority" key
        
        if training_instances == "support":
            i = -1
            for t in data_txt: #for each data group extracted from each txt file... - 6 iter
                i += 1
                for p in t: #for each "priority set" (L0,L1,L2,L3) related to the txt file... - 4 iter
                    for freq_set in p: #and for each frequent set (i.e the tuples that represent the extracted frequent sets) in a priority set... - n iter
                        iters = int(freq_set[2]) - (freq_set[3] - 1) # the support generates duplicates
                        for s in range(1, iters):
                            if priority_node:
                                dict_data['priority'].append(freq_set[0])
                            for ud in variable_names: # - m iter
                                if ud in freq_set[1] or ud==file_names[i]:
                                    value = 1 #in this set, the device has triggered an event
                                else:
                                    value = 0 #the device hasn't triggered a event
                                        
                                if ud in dict_data: #key already present
                                    dict_data[ud].append(value)
                                else: #create new key in dictionary
                                    dict_data[ud] = []
                                    dict_data[ud].append(value)
                                
        elif training_instances == "all_events":
            for key in events_by_file:
                for tupl in events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0], priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        if priority_node:
                            dict_data['priority'].append(tupl[1])
                        for ud in variable_names:
                            if ud in tupl[0] or ud==key:
                                value = 1
                            else:    
                                value = 0
                                
                            if ud in dict_data: #key already present
                                dict_data[ud].append(value)
                            else: #create new key in dictionary
                                dict_data[ud] = []
                                dict_data[ud].append(value)
                            
        elif training_instances == "all_events_priority":
            for key in events_by_file:
                for tupl in events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0], priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        if priority_node:
                            dict_data['priority'].append(tupl[1])
                        for ud in variable_names:
                            if ud in tupl[0] or ud==key:
                                value = tupl[1]
                            else:    
                                value = 0
                                
                            if ud in dict_data: #key already present
                                dict_data[ud].append(value)
                            else: #create new key in dictionary
                                dict_data[ud] = []
                                dict_data[ud].append(value)
                            
        elif training_instances == "all_events_with_causes":
            for key in events_by_file:
                for tupl in events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0], priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        if priority_node:
                            dict_data['priority'].append(tupl[1])
                        for ud in variable_names:
                            if ud in tupl[0] or self.check_trigger(ud, key):
                                value = 1
                            else:    
                                value = 0
                                
                            if ud in dict_data: #key already present
                                dict_data[ud].append(value)
                            else: #create new key in dictionary
                                dict_data[ud] = []
                                dict_data[ud].append(value)
        
        else:
            print("training_instances generation method not chosen correctly")
            return
        data = pd.DataFrame(dict_data)
        return data
    
    def build_libpgm_data(self, training_instances='none', priority_node = False):
        ''' Builds and return the array of dictionaries required by the libpgm's PGMlearner class.
            training_instances = 
            all_events -- to generate one training instance per event (in "distinct devices after 5 minutes")
        '''
        array_data = []
        if training_instances == 'all_events':
            for key in events_by_file:
                for tupl in events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0],priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        dict_data = dict()
                        if priority_node:
                            dict_data['priority'] = tupl[1]
                        for ud in variable_names:
                            if ud in tupl[0] or ud==key:
                                value = 1
                            else:    
                                value = 0
                            dict_data[ud] = value
                        array_data.append(dict_data)
        else:
            print("training_instances generation method not chosen correctly")
            return
        return(array_data)
    
    def build_numpy_data(self, training_instances='none', priority_node = False):
        '''     
        Builds and returns the numpy array used by the pyBn library       
        training_instances = 
        all_events -- to generate one training instance per event (in "distinct devices after 5 minutes")
        '''
        list_of_lists = []
        single_list = []
        #To create the numpy array we use a list of list. The first list has the column headers (nodes).
        if priority_node:
            single_list.append(priority)
        for ud in variable_names:
            single_list.append(ud)
        list_of_lists.append(single_list)
        
        if training_instances == "all_events":
            for key in events_by_file:
                for tupl in events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0],priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        single_list = []
                        if priority_node:
                            single_list.append(tupl[1]) #add the priority
                        for ud in variable_names: 
                            if ud in tupl[0] or ud==key:
                                value = 1
                            else:    
                                value = 0
                            single_list.append(value) #This works if the "for" cycle over variable_names is always done in the same order
                        list_of_lists.append(single_list)
        else:
            print("training_instances generation method not chosen correctly")
            return
        data = np.array(list_of_lists)
        return data
        
        
    def not_empty_check(self, device_list, priority_node):
        ''' Takes a list of devices and checks if at least one variable is present in that list.
        If the priority node is not present, will required that at least 2 devices are present
        '''
        global variable_names
        if priority_node:
            i = 1
        else:
            i = 0
        for d in variable_names:
            if d in device_list:
                i += 1
                if i >=2:
                    return True
        return False
    
    def get_data_txt(self):
        return data_txt
    
    def check_trigger(self, name, key):
        ''' 
        Checks if the given device name is a "cause" by searching for the "trigger" keyword.
        It then checks if the given key (i.e. the true name of the device file) corresponds to that device name.
        '''
        check = re.compile('trigger').findall(name)
        if check:
            device = name.partition("_")[2]
            if key == device:
                return device
                
    def get_variable_names(self):
        return variable_names  
    
    def get_unique_frequent_devices_by_file(self):
        return unique_frequent_devices_by_file
    
    def add_variable_names(self, names):
        global variable_names
        for n in names:   
            variable_names.append(n)
                