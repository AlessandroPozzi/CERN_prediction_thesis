'''
Created on 15 nov 2017
@author: Alessandro Pozzi, Lorenzo Costantini
'''

import re
import pandas as pd
import columnAnalyzer as colAnal
from DatabaseNetworkCorrelator import DatabaseNetworkCorrelator
import config

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

    def extract(self, txtfile, true_device_name, select_priority, file_suffix):
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
        
        with open ('../../res/' + txtfile + file_suffix + '.txt', 'r') as in_file:
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
        Stores the devices and extra in the given line (which should be found in the "Distinct devices after 5 minutes" set for later use 
        and also stores the priority
        '''
        devices_extra_found = self.find_devices(line)
        #if devices_extra_found:
        tupl = (devices_extra_found, p)
        self.events_by_file[file_name].append(tupl)
        self.totalRows = self.totalRows + 1
            
    def find_devices(self, line):
        ''' 
        Finds all the devices ID in the given line. Returns a list of tuples (device, extra) 
        '''
        #findings = re.compile('\'\S+\'').findall(line)
        findings = re.compile('\'(.*?)\'').findall(line)
        processed_findings = []
        for f in findings:
            device = re.compile('(.*?)\-\-').findall(f)
            extra = [""]
            #device[0].replace("--", "")
            #device[0].replace("'", "")
            if config.EXTRA:
                extra = re.compile('\-\-(.*?)\Z').findall(f)
                #extra[0].replace("--", "")
                extra[0].replace("'", "")
            processed_findings.append((device[0], extra[0]))
        return processed_findings
    
    def prepare_candidates(self, var_type, manualList):
        '''
        Extracts occurrences and frequency of the devices (i.e. the candidate for becoming variables of the network).
        Occurrences and frequency are computed by looking ONLY at the txt itemsets.
        This data is stored in ranked_devices, as a tuple of the kind (device--extra), frequency, occurrences, average, st.dev.).
        '''
        self.ranked_devices = []
        self.mostFrequentDevInCouples = dict()
        frequency_by_device = dict() # key = device, value = sum of frequencies of device
        allDevicesExtra = set() #contain couples (device, extra)
        couple_occurrences = dict()
        self.manualList = manualList

        for key in self.events_by_file:
            occurrences = dict() #key = device; value = number of occurrences in SINGLE FILE
            total_events = len(self.events_by_file[key])
            for tupl in self.events_by_file[key]: # each tuple is: (device_list, priority)
                if config.occurrencesAsBN:
                    maxOnce = dict() #max once couple per sequence/itemset
                for index, couple in enumerate(tupl[0]):
                    devExtra = couple[0] + "--" + couple[1]
                    allDevicesExtra.add(couple)
                    if (config.occurrencesAsBN and devExtra not in maxOnce) or not config.occurrencesAsBN:
                        if devExtra not in occurrences: #create new key
                            occurrences[devExtra] = 1
                        else: #update key
                            occurrences[devExtra] = occurrences[devExtra] + 1
                    #part related to couple_occurrences criterion
                    if config.occurrencesAsBN:
                        maxOnce[devExtra] = True
                    next = index + 1
                    if next < len(tupl[0]):
                        futureCouple = (tupl[0][index], tupl[0][next])
                        if futureCouple not in couple_occurrences:
                            couple_occurrences[futureCouple] = 1
                        else:
                            couple_occurrences[futureCouple] = couple_occurrences[futureCouple] + 1
            for devExtra in occurrences:
                frequency_by_device[devExtra] = round( occurrences[devExtra] / float(total_events) , 2)

        self.devicesColumnDict = colAnal.find_column_distribution(self.true_file_names[0], self.priority_selected, list(allDevicesExtra))
        self.occurrences = occurrences

        if var_type == "occurrences" or var_type == "frequency" or var_type == "manual":
            for de in frequency_by_device:
                tupl = (de, frequency_by_device[de], occurrences[de], -1, -1)
                self.ranked_devices.append(tupl)
        elif var_type == "variance_only" or var_type == "support_variance":
            for de in frequency_by_device:
                tupl = (de, frequency_by_device[de], occurrences[de],
                        self.devicesColumnDict[de].msAverage / 1000, self.devicesColumnDict[de].msStandDev / 1000)
                self.ranked_devices.append(tupl)
        elif var_type == "lift":
            dnc = DatabaseNetworkCorrelator()
            dnc.initAsPreProcessor(self.true_file_names[0], self.priority_selected, log = True)
            lift = dnc.totalOccurrencesCandidatesAnalysis(list(allDevicesExtra))
            for de in lift:
                tupl = (de, frequency_by_device[de], occurrences[de],
                        "lift:", lift[de])
                self.ranked_devices.append(tupl)
        elif var_type == "couple_occurrences":
            couple_occurrences_list = couple_occurrences.items()
            '''
            # divide occurrences for the support of the first couple
            normalized_occ_list = []
            for coup in couple_occurrences_list:
                currCoupleSorg = (coup[0][0][0], coup[0][0][1])
                supportSorg = 0
                for coup2 in couple_occurrences_list:#count the support of the couple
                    if currCoupleSorg == (coup2[0][0][0], coup2[0][0][1]):
                        supportSorg = supportSorg + coup2[1]
                if currCoupleSorg not in normalized_occ_list:
                    for coup2 in couple_occurrences_list:
                        if supportSorg < 10:# apply minimum threshold for support
                            coup2[1] = 0
                        else:
                            if currCoupleSorg == (coup2[0][0][0], coup2[0][0][1]):
                                coup2[1] = coup2[1] / supportSorg
                normalized_occ_list.append(currCoupleSorg)
            # --- END of this part
            '''
            couple_occurrences_list.sort(key=lambda tup: tup[1],
                                              reverse=True)  # order by occurrences of consecutive couples
            #don't consider self loops for this criterion
            couple_occurrences_list_no_self = [fullCouple for fullCouple in couple_occurrences_list if not
                                               (fullCouple[0][0][0] == fullCouple[0][1][0] and fullCouple[0][0][1] == fullCouple[0][1][1])]
            #take only the most 10, 15, 20 (choose a value) frequent consecutive couples
            couple_occurrences_list_no_self = couple_occurrences_list_no_self[:10]
            for fullCouple in couple_occurrences_list_no_self:
                deviceExtra1 = fullCouple[0][0][0] + "--" + fullCouple[0][0][1]
                deviceExtra2 = fullCouple[0][1][0] + "--" + fullCouple[0][1][1]
                if deviceExtra1 not in self.mostFrequentDevInCouples:
                    self.mostFrequentDevInCouples[deviceExtra1] = fullCouple[1]
                else:
                    self.mostFrequentDevInCouples[deviceExtra1] = self.mostFrequentDevInCouples[deviceExtra1] + fullCouple[1]
                if deviceExtra2 not in self.mostFrequentDevInCouples:
                    self.mostFrequentDevInCouples[deviceExtra2] = fullCouple[1]
                else:
                    self.mostFrequentDevInCouples[deviceExtra2] = self.mostFrequentDevInCouples[deviceExtra2] + fullCouple[1]
            for de in self.mostFrequentDevInCouples:
                tupl = (de, frequency_by_device[de], occurrences[de],
                        self.devicesColumnDict[de].msAverage / 1000, self.devicesColumnDict[de].msStandDev / 1000,
                        self.mostFrequentDevInCouples[de]) #it's a 6-tuple, instead of 5-tuple
                self.ranked_devices.append(tupl) #not yet ranked
            

                
        
    def select_candidates(self, var_type, support, MIN, MAX):
        '''
        Selects the devices to be used as variables of the network among the candidates previously selected.
        "var_type" can be:
        "occurrences" -- Select the "MAX" variables with most occurrences.
        "frequency" -- Select the "MIN" variables with highest support plus a maximum of ("MAX"-"MIN") variables
                       that have a support higher than the "support" parameter.
        "variance_only" -- Select the "MIN" variables with highest standard deviation, plus a maximum of ("MAX"-"MIN") variables
                       that have a variance higher than a fixed value (ex: 30 seconds).
        "support_variance" -- Select the "MIN" variables with highest support plus a number ("MAX"-"MIN") of remaining variables
                              with the highest standard deviation.
        "couple_occurrences" -- Select the "MAX" variables that appear most frequently in the most frequent couples of consecutive
                              devices.
        '''

        if var_type == "occurrences":
            self.ranked_devices.sort(key = lambda tup: tup[2], reverse=True) #order by occurrences
        elif var_type == "frequency" or var_type == "support_variance" or var_type == "manual":
            self.ranked_devices.sort(key = lambda tup: tup[1], reverse=True) #order by support
        elif var_type == "variance_only":
            self.ranked_devices.sort(key = lambda tup: tup[4]) #order by variance (actually, standard deviation)
        elif var_type == "lift":
            self.ranked_devices.sort(key = lambda tup: tup[4], reverse=True) #order by lift
        elif var_type == "couple_occurrences":
            self.ranked_devices.sort(key = lambda tup: tup[5], reverse=True)

        #ordered_ranking = [i for i in self.ranked_devices if i[0] not in self.true_file_names[0]] # helper list with no file device in it ----THIS HAS TO BE UPDATED!!!!!!!!!
        ordered_ranking = []
        for varRanked in self.ranked_devices:
            dev = re.compile('(.*?)\-\-').findall(varRanked[0])
            dev = dev[0].replace("--", "")
            dev = dev.replace("'", "")
            if dev not in self.true_file_names[0]:
                ordered_ranking.append(varRanked)

        min_occ = 5
        if var_type == "variance_only" or var_type == "lift":
            ordered_ranking = [tup for tup in ordered_ranking if tup[2] > min_occ] #remove devices with less than "min_occ" occurrences

        if var_type == "manual": #bypass everything and select variables manually
            for dev in self.manualList:
                self.variable_names.append(dev)
            self.ranked_devices = ordered_ranking
            return

        for i in range(len(ordered_ranking)):
            NUM = len(self.variable_names)
            deviceExtra = ordered_ranking[i][0]
            frequency = ordered_ranking[i][1]
            stDev = ordered_ranking[i][4] * 1000

            if NUM < MIN:
                self.variable_names.append(deviceExtra)
            elif NUM < MAX:
                if var_type == "frequency":
                    if frequency > support:
                        self.variable_names.append(deviceExtra)
                elif var_type == "occurrences" or var_type == "couple_occurrences":
                    self.variable_names.append(deviceExtra)
                elif var_type =="variance_only":
                    max_stdev_seconds = 30
                    if stDev < 1000 * max_stdev_seconds:
                        self.variable_names.append(deviceExtra)
                elif var_type == "support_variance":
                    ordered_ranking = [x for x in ordered_ranking if x[0] not in self.variable_names] #remove devices already added
                    ordered_ranking.sort(key = lambda tup: tup[4]) #order by variance
                    for j in range(NUM, MAX):
                        self.variable_names.append(ordered_ranking[j - NUM][0])
                    break
                elif var_type == "lift":
                    if (stDev / 100) >= 0.5: #stDev here is actually the lift in %
                        self.variable_names.append(deviceExtra)
            else:
                break

        self.ranked_devices = ordered_ranking

    
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
                    deviceExtraList = [x[0]+"--"+x[1] for x in tupl[0]] #build the "device--state" string
                    for ud in self.variable_names:
                        if ud in deviceExtraList or ud==key:
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
        data.to_csv(path_or_buf="../../output/dataframes/" + self.txt_file_names[0] + "_" + 
                    self.priority_selected + "_" + "dataframe.csv")
        
        return data

        
    def not_empty_check(self, tuples_list):
        ''' Takes a list of devices + extras and checks if at least one variable is present in that list.
        If the priority node is not present, will required that at least 2 devices are present
        '''
        '''
        deviceExtra_list = [(x[0] + "--" + x[1]) for x in tuples_list]
        i = 0
        for d in self.variable_names:
            if d in deviceExtra_list:
                i += 1
                if i >=1: #i>=x : accepts training instances with at least x "1".
                    return True
        self.skipped_lines += 1
        return False
        '''
        return True
    
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
        for tpl in self.events_by_file[fl]:
            row = tpl[0]
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
            
    def get_columnAnalysis(self):
        return self.devicesColumnDict
    
    def get_occurrences(self):
        return self.occurrences
                