'''
Created on 15 nov 2017

@author: Alessandro Corsair
'''

import re
import pandas as pd
import numpy as np



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
        self.data_txt = [] #This list will contain "data_p" elements, one per txt file
        # data_p = [[], [], [], []] this list contains 4 elements, one for each priority. 
        #Each element is a list of tuples of the type: (priority, devices, support). The devices are a list of frequent devices with that particular priority and support
        self.variable_names = [] #to be used as nodes in the network
        self.true_file_names = [] #stores the names of the files
        self.txt_file_names = []
        self.priority_selected = []
        self.events_by_file = dict() #a dictionary that has filenames as keys, and a list of tuples as values. Each tuple is a couple ([device list], priority)
        self.ranked_devices = [] #list of tuples that show which devices appear more
        self.unique_frequent_devices_by_file = dict() #
        self.skipped_lines = 0
        self.totalRows = 0

    def extract(self, txtfile, true_device_name, select_priority = []):
        '''
        Extracts data from the txt file
        
        Parameters
        ----------
        txtfile: the name of the txt file found in the /res folder
        true_device_name : the real name of the device in the events
        select_priority : a list that contain the priority levels (L0,L1,L2,L3) to be considered
        '''
        data_p = [[], [], [], []]
        self.true_file_names.append(true_device_name)
        self.txt_file_names.append(txtfile)
        self.priority_selected = select_priority
        self.events_by_file[true_device_name] = []
        self.unique_frequent_devices_by_file[true_device_name] = []
        
        with open ('../res/' + txtfile +'.txt', 'r') as in_file:
            all_events = 1
            p = 0
            min_support = 0
            
            for line in in_file.readlines():
                #NOTE: the order of IF conditions IS RELEVANT
                
                if all_events==1 and self.priority[p-1] in select_priority: # here we are in the "Distinct devices after 5 min"
                    self.store_line(line, true_device_name, self.priority[p-1])
                
                if re.search(r"PRIORITY", line): #Here we are in the "PRIORITY level" line
                    p+=1 #priority level
                    all_events = 1
                
                if all_events==0 and self.priority[p-1] in select_priority:
                    match = self.find_devices(line)
                    support = self.find_support(line)
                    if match and support:
                        content = (self.priority[p-1], match, support[0], min_support)
                        data_p[p-1].append(content)
                        self.unique_frequent_devices(match, true_device_name)
                        
                if re.search(r"==>", line):
                    all_events = 0 #i.e. we'll now see the processed frequent itemsets
                    min_support = self.find_min_support(line)
        
        self.data_txt.append(data_p)
            
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
        for dev in devices:
            if dev not in self.variable_names:
                self.variable_names.append(dev)
            if dev not in self.unique_frequent_devices_by_file[file_name]:
                self.unique_frequent_devices_by_file[file_name].append(dev)
                

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
            
    def count_occurrences_all_devices(self):
        ''' Finds all the unique devices in all the events '''
        self.ranked_devices = []
        self.device_occurrences = dict() #this will help storing the # of occurrences
        
        for key in self.events_by_file:
            for line in self.events_by_file[key]:
                for d in line[0]: #[0] because it's a tuple
                    if d not in self.priority: #we don't want to count the priority
                        if d in self.device_occurrences:
                            self.device_occurrences[d] = self.device_occurrences[d] + 1 #update key
                        else:
                            self.device_occurrences[d] = 1 #create key
        
        for key in self.device_occurrences:
            self.ranked_devices.append((key, self.device_occurrences[key]))
        
        self.ranked_devices.sort(key = lambda tup: tup[1], reverse=True)
        
        return self.ranked_devices 
        
        
    def count_occurrences_variables(self):
        ''' Counts the occurrences of the variable names in all the "Distinct devices after 5 minutes" set '''
        device_occurrences = dict() #this will help storing the # of occurrences
        
        for key in self.events_by_file:
            for tupl in self.events_by_file[key]:
                for d in tupl[0]:
                    if d not in device_occurrences: #create new key
                        device_occurrences[d] = 1
                    else: #update key
                        device_occurrences[d] = device_occurrences[d] + 1
                    
        for key in device_occurrences:
            self.ranked_devices.append((key, device_occurrences[key]))
        
        self.ranked_devices.sort(key = lambda tup: tup[1], reverse=True)
        return self.ranked_devices 
    
    def frequency_occurences_variables(self):
        ''' Returns the all the devices ordered by their summed individual (in files) frequency '''
        self.ranked_devices = []
        frequency_by_device = dict() # key = device, value = sum of frequencies of device

        for key in self.events_by_file:
            occurrences_in_file = dict() #key = device; value = number of occurrences in SINGLE FILE
            total_events = len(self.events_by_file[key])
            for tupl in self.events_by_file[key]:
                for d in tupl[0]:
                    if d not in occurrences_in_file: #create new key
                        occurrences_in_file[d] = 1
                    else: #update key
                        occurrences_in_file[d] = occurrences_in_file[d] + 1
            for d in occurrences_in_file:
                if d not in frequency_by_device:
                    frequency_by_device[d] = occurrences_in_file[d] / float(total_events)
                else:
                    frequency_by_device[d] = frequency_by_device[d] + occurrences_in_file[d] / float(total_events)
        
        for d in frequency_by_device:
            tupl = (d, frequency_by_device[d])
            self.ranked_devices.append(tupl)
        
        self.ranked_devices.sort(key = lambda tup: tup[1], reverse=True)
        return self.ranked_devices
    
    def take_n_variables_count(self, n):
        ''' choses the n most frequent devices and uses them as new variables '''
        self.variable_names = []
        add_one = False
        i = 0
        while i < n:
            candidate = self.ranked_devices[i][0]
            if candidate == self.true_file_names[0]:
                add_one = True
            else:
                self.variable_names.append(self.ranked_devices[i][0])
            i = i + 1
        
        if add_one:
            print(self.true_file_names[0] + " device variable skipped")
            self.variable_names.append(self.ranked_devices[i][0])
        
    def take_n_variables_support(self, var_type, support, filter = ""):
        self.variable_names = []
        if var_type == "all_count":
            for i in range(len(self.ranked_devices)):
                if self.ranked_devices[i][1] > support * self.totalRows:
                    self.variable_names.append(self.ranked_devices[i][0])
        elif var_type == "all_frequency":
            refused = [] #list of devices not taken because of low support (except device of file)
            for i in range(len(self.ranked_devices)):
                rank = self.ranked_devices[i][1]
                device = self.ranked_devices[i][0]
                if  rank > support and device != self.true_file_names[0]:
                    self.variable_names.append(device)
                elif rank < support and device != self.true_file_names[0]:
                    refused.append(device)
            if filter == "support_bound": # checks that >= MIN and <= MAX vars are taken
                NUM = len(self.variable_names)
                MIN = 4
                MAX = 6
                if MIN > NUM:
                    self.variable_names.extend(refused[:(MIN-NUM)])
                if NUM > MAX:
                    self.variable_names = self.variable_names[: - (NUM - MAX)]
                
    
    def reset_variable_names(self, new_list):
        ''' Replaces the unique devices with a given list '''
        self.variable_names = new_list
    
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
            for t in self.data_txt: #for each data group extracted from each txt file... - 6 iter
                i += 1
                for p in t: #for each "priority set" (L0,L1,L2,L3) related to the txt file... - 4 iter
                    for freq_set in p: #and for each frequent set (i.e the tuples that represent the extracted frequent sets) in a priority set... - n iter
                        iters = int(freq_set[2]) - (freq_set[3] - 1) # the support generates duplicates
                        for s in range(1, iters):
                            if priority_node:
                                dict_data['priority'].append(freq_set[0])
                            for ud in self.variable_names: # - m iter
                                if ud in freq_set[1] or ud==self.true_file_names[i]:
                                    value = 1 #in this set, the device has triggered an event
                                else:
                                    value = 0 #the device hasn't triggered a event
                                        
                                if ud in dict_data: #key already present
                                    dict_data[ud].append(value)
                                else: #create new key in dictionary
                                    dict_data[ud] = []
                                    dict_data[ud].append(value)
                                
        elif training_instances == "all_events":
            for key in self.events_by_file:
                for tupl in self.events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0], priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        if priority_node:
                            dict_data['priority'].append(tupl[1])
                        for ud in self.variable_names:
                            if ud in tupl[0] or ud==key:
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
                
                            
        elif training_instances == "all_events_priority":
            for key in self.events_by_file:
                for tupl in self.events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0], priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        if priority_node:
                            dict_data['priority'].append(tupl[1])
                        for ud in self.variable_names:
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
            for key in self.events_by_file:
                for tupl in self.events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0], priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        if priority_node:
                            dict_data['priority'].append(tupl[1])
                        for ud in self.variable_names:
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
        ''' Only for github issue page:
        aliases = ['Node1', 'Node2', 'Node3', 'Node4', 'Node5', 'Node6', 'Node7', 'Node8']
        data.to_csv(path_or_buf="../res/pandas_dataframe.csv", header=aliases)
        '''
        data.to_csv(path_or_buf="../output/" + self.txt_file_names[0] + "_" + 
                    self.priority_selected[0] + "_" + "dataframe.csv")
        return data
    
    def build_libpgm_data(self, training_instances='none', priority_node = False):
        ''' Builds and return the array of dictionaries required by the libpgm's PGMlearner class.
            training_instances = 
            all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes")
        '''
        array_data = []
        if training_instances == 'all_events':
            for key in self.events_by_file:
                for tupl in self.events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0],priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        dict_data = dict()
                        if priority_node:
                            dict_data['priority'] = tupl[1]
                        for ud in self.variable_names:
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
        Builds and returns the numpy array used by the pomegranate library       
        training_instances = 
        all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes")
        all_events_with_causes -- to add the trigger variables
        
        To create the numpy array we use a list of list. The first list has the column headers (nodes).
        if priority_node:
            single_list.append(priority)
        for ud in variable_names:
            single_list.append(ud)
        list_of_lists.append(single_list)
        '''
        list_of_lists = []
        single_list = []
        if training_instances == "all_events":
            for key in self.events_by_file:
                for tupl in self.events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0],priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        single_list = []
                        for ud in self.variable_names: 
                            if ud in tupl[0] or ud==key:
                                value = 1
                            else:    
                                value = 0
                            single_list.append(value) #This works if the "for" cycle over variable_names is always done in the same order
                        if priority_node:
                            single_list.append(tupl[1]) #add the priority
                        list_of_lists.append(single_list)         
                        
        elif training_instances == "all_events_with_causes":
            for key in self.events_by_file:
                for tupl in self.events_by_file[key]: #each "line" is a list of an event sequence (+ priority as the other element of the tuple) to be turned into a training instance
                    if self.not_empty_check(tupl[0],priority_node): #i.e. consider only events lines that generate a NON-EMPTY training instance
                        single_list = []
                        for ud in self.variable_names:
                            if ud in tupl[0] or self.check_trigger(ud, key): #or ud==key omitted
                                value = 1
                            else:    
                                value = 0
                            single_list.append(value)
                        if priority_node:
                            single_list.append(tupl[1]) #add the priority
                        list_of_lists.append(single_list)
        
        else:
            print("training_instances generation method not chosen correctly")
            return
        if priority_node:
            self.variable_names.append('priority')
        data = np.array(list_of_lists)
        return data
        
        
    def not_empty_check(self, device_list, priority_node):
        ''' Takes a list of devices and checks if at least one variable is present in that list.
        If the priority node is not present, will required that at least 2 devices are present
        '''
        if priority_node:
            i = 1
        else:
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
            
    def nodata(self):
        ''' 
        Checks if there is some data in this priority - file combination. 
        '''
        file = self.true_file_names[0]
        if self.events_by_file[file] == []:
            return True
        for row in self.events_by_file[file]:
            for device in row:
                if device != file:
                    return False
        return True
                
    def get_variable_names(self):
        return self.variable_names  
    
    def get_ranked_devices(self):
        return self.ranked_devices
    
    def get_unique_frequent_devices_by_file(self):
        return self.unique_frequent_devices_by_file
    
    def add_variable_names(self, names):
        for n in names:   
            self.variable_names.append(n)
                