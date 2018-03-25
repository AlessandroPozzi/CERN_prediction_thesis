'''
Created on 09 gen 2018

@author: Alessandro Corsair
'''
'''
from networkgeneration.Data_extractor import Data_extractor
from helpers.DataError import DataError
from helpers.File_writer import File_writer
'''
from Data_extractor import Data_extractor
from DataError import DataError
from File_writer import File_writer
import config

class Pre_network_handler(object):
    
    def __init__(self, gh):
        
        self.file_names = config.escaped_file_names
        self.true_device_names = config.true_device_names
        self.extractor = Data_extractor()
        self.general_handler = gh
        self.data = []
        
    def process_files(self, select_priority, file_selection, file_suffix, log = True):
        '''  (1)
        Method that extracts data from the text files.
        -----------------
        Parameters:
        select_priority -- A string with the priority level to be considered
        files_used      -- The number of the file to be used
        log             -- "True" if you want to print debug information in the console
        '''
        if not select_priority or not file_selection:
            raise DataError("Priority or file not chosen. Exiting now.")

        num = file_selection
        self.extractor.extract(self.file_names[num-1], self.true_device_names[num-1], select_priority, file_suffix)
        self.device_considered = self.file_names[num-1]
        self.device_considered_realName = self.true_device_names[num-1]
        self.priority_considered = select_priority
        self.file_suffix = file_suffix
        
        self.file_writer = File_writer(self.device_considered, self.priority_considered)
        self.log("Priority level considered: " + select_priority, log)
        self.log("File considered: " + str(self.file_names[num-1]), log)
        self.log("Text files data extraction completed.", log)
    
    
    def select_variables(self, var_type, MIN, MAX, support, log = True, manualList = None):
        ''' (2)
        Method that selects the variables to be used in the network.
        -----------------
        Parameters:
        var_type  : The origin of the variables to be considered. Accepted values:
           -> occurrences       - If we consider the devices 
           -> frequency   - If we consider the frequency of the devices
        MIN: Minimum number of variables to take
        MAX: Maximum number of variables to take
        support   : Minimum support to consider the device in the final Bayesian Network
        log       : "True" if you want to print debug information in the console
        '''
        if self.extractor.nodata():
            raise DataError("No data in this file - priority")
            
        self.extractor.prepare_candidates(var_type, manualList) #computes occurrences and frequency of the devices
        self.extractor.select_candidates(var_type, support, MIN, MAX)
        self.devicesColumnDict = self.extractor.get_columnAnalysis()
        self.occurrences = self.extractor.get_occurrences()
        
        if self.general_handler:
            self.general_handler.add_devices(self.extractor.get_variable_names())
        self.log(self.extractor.get_ranked_devices(), log)
  
  
    def build_data(self, training_instances="all_events", log=True):
        ''' (3)
        Method that builds the data to be used by the graphical model library.
        -----------------
        Parameters:
        training_instances : 
            support                -- to use duplicated training instances based on the support of the frequent sets
            all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes")
            all_events_with_causes -- like all_events, but also considers the 6 causes variables
            all_events_priority    -- like all_event but instead of using [0, 1] as values for variables, uses the priority related to the event: [0, L0, L1, L2, L3]
        priority_node   : True if you want the priority node, false otherwise.
        log       : "True" if you want to print debug information in the console    
        '''
        
        variables = self.extractor.get_variable_names()
        self.training_instances = training_instances
        self.data = self.extractor.build_dataframe(training_instances)
        
        self.log("There are " + str(len(variables)) + " variables in the network: " + " ".join(variables), log)
        self.log("Library used: pgmpy", log)
        self.log("There are " + str(len(self.data)) + " 'training' instances in the dataframe.", log)    
        return self.data
    
    def checkColumnDistribution(self):
        ''' (*)
        Extracts some info about the distribution of state, tag and description in the devices in this network '''
        import columnAnalyzer
        columnAnalyzer.find_column_distribution(self.device_considered_realName, self.priority_considered, 
                                 self.extractor.get_variable_names(), write = True)
        
    def get_data_extractor(self):
        return self.extractor
    
    def get_dataframe(self):
        return self.data
    
    def get_device(self):
        return self.device_considered
    
    def get_priority(self):
        return self.priority_considered
    
    def get_file_writer(self):
        return self.file_writer
    
    def get_file_suffix(self):
        return self.file_suffix
    
    def get_column_analysis(self):
        return self.devicesColumnDict
    
    def get_occurrences(self):
        return self.occurrences
    
    def log(self, text, log):
        ''' Prints the text in the console, if the "log" condition is True. '''
        if log:
            print(text)
