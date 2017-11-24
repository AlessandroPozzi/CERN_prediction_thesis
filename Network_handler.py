'''
Created on 23 nov 2017

@author: Alessandro Corsair
'''

import networkx as nx
from Data_extractor import Data_extractor, file_names
from pgmpy.estimators import HillClimbSearch, BicScore, BayesianEstimator, K2Score
from pgmpy.models import BayesianModel
import matplotlib.pyplot as plt
from pgmpy.inference import VariableElimination
from pgmpy.estimators.ExhaustiveSearch import ExhaustiveSearch
import json
from libpgm.nodedata import NodeData
from libpgm.graphskeleton import GraphSkeleton
from libpgm.discretebayesiannetwork import DiscreteBayesianNetwork
from libpgm.pgmlearner import PGMLearner
from Data_extractor import Data_extractor

class Network_handler:
    '''
    Handles creation and usage of the probabilistic network over CERN's data.
    Note that the method of this class have numbers and must be called in order.
    '''

    def __init__(self):
        '''
        Constructor
        '''
        file_names = ["EHS60BE","EMC0019", "ES115H","ESS184","EXS48X","EXS1062X"]
        true_device_names = ['EHS60/BE', 'EMC001*9', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
        extractor = Data_extractor()
        lib = ""
        learner = PGMLearner()
        graph_skeleton = GraphSkeleton()
        best_model = BayesianModel()


    def process_files(self, ignore_priority = [], files_used = 6, log = True):
        '''  (1)
        Method that extracts data from the text files.
        -----------------
        Parameters:
        ignore_priority -- list of the priority levels that must be completely ignored
        files_used      -- the number of files to be used (min 1, max 6)
        log             -- "True" if you want to print debug information in the console
        '''
        if files_used > 6 or files_used < 1:
            print("file_used parameter incorrect")
            return
        else:   
            i = 0 
            for name, true_name in zip(self.file_names, self.true_device_names):
                if i == files_used:
                    return
                else:
                    self.extractor.extract(name, true_name, ignore_priority)
                i += 1

        if log:
            print("Text files data extraction completed.")

    
    def select_variables(self, var_type, var_num = 6, extra_var = "none", log = True):
        ''' (2)
        Method that selects the variables to be used in the network.
        -----------------
        Parameters:
        var_type  -- The origin of the variables to be considered. Accepted values:
                   -> all       - If we consider the devices from the complete event list
                   -> file_name - If we consider only the 6 file devices as variables
        var_num   -- How many variables to take.
        extra_var -- Adds extra variables.
                   -> none      - Default value. No extra variable is taken.
                   -> causes    - To add the 6 extra variables corresponding to the 6 file devices.
        log       -- "True" if you want to print debug information in the console    
        '''
        if var_type == "all":
            ordered_list = self.extractor.count_occurrences_variables()
            self.extractor.take_n_variables(var_num)
            if log:
                print(ordered_list)
                
        elif var_type == "file_name":
            self.extractor.reset_variable_names(self.true_device_names) 
            if log:
                print("We consider only 6 devices")
                
        if extra_var == "causes":
            causes = ['trigger_EHS60/BE', 'trigger_EMC001*9', 'trigger_ESS11/5H', 'trigger_ESS1*84', 'trigger_EXS4/8X', 'trigger_EXS106/2X']
            self.extractor.add_variable_names(causes)
            
            
    def build_data(self, library, training_instances="all_events", log=True):
        ''' (3)
        Method that builds the data to be used by the graphical model library.
        -----------------
        Parameters:
        library            -- The library for which the data is formatted
           -> pgmpy 
           -> libpgm 
        training_instances -- (for pgmpy)
            support                -- to use duplicated training instances based on the support of the frequent sets
            all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes")
            all_events_with_causes -- like all_events, but also considers the 6 causes variables
            all_events_priority    -- like all_event but instead of using [0, 1] as values for variables, uses the priority related to the event: [0, L0, L1, L2, L3]
        training_instances -- (for libpgm)    
            all_events -- to generate one training instance per event (in "distinct devices after 5 minutes")
        log       -- "True" if you want to print debug information in the console    
        '''
        self.lib = library
        variables = self.extractor.get_variable_names()
        if log:
            print("There are " + str(len(variables)) + " variables in the network: " + " ".join(variables))
            print("Library used: " + library)
        
        if library == "pgmpy":
            data = self.extractor.build_dataframe(training_instances)
            if log:
                print("There are " + str(len(data)) + " 'training' instances in the dataframe.")    
        
        elif library == "libpgm":
            data = self.extractor.build_libpgm_data(training_instances)
            if log:
                print("There are " + str(len(data.keys())) + " 'training' instances")
        
        
        def learn_structure(self, method, scoring_method, prior = "none", log = True):
            ''' (4)
            Method that builds the structure of the data
            -----------------
            Parameters: (only for pgmpy)
            method          -- The technique used to search for the structure
                -> scoring  -- To use a scoring method
            scoring_method  -- (only for scoring method)
                -> K2
                -> bic
            prior           -- Initial condition for the structure (only for pgmpy)
                -> none     -- default
                -> priority -- Start with edges between priority and the 6 main devices 
            log             -- "True" if you want to print debug information in the console    
            '''
            if self.lib == "libpgm":
                self.graph_skeleton = self.learner.discrete_constraint_estimatestruct(data)
            
            elif self.lib == "pgmpy":
                if scoring_method == "K2":
                    scores = K2Score(data)
                elif scoring_method == "bic":
                    scores = BicScore(data)
                if log:
                    print("Search for best approximated structure started...")
                est = HillClimbSearch(data, scores)
                
                if prior == "priority":
                    if log:
                        print("Forcing priority edges...")
                    start_model = BayesianModel()
                    start_model.add_nodes_from(self.extractor.get_variable_names())
                    for d in self.true_device_names:
                        if d in self.extractor.get_variable_names():
                            start_model.add_edge('priority', d)
                    self.best_model = est.estimate(start=start_model)
                elif prior == "none":
                    self.best_model = est.estimate()
                
            if log:
                print("Search terminated")
                
            else:
                print("Error in choosing the library")
                return
            

        
        def estimate_parameters(self, log=True):
            ''' (5)
            Estimates the parameters of the found network
            '''
            if self.lib == "libpgm":
                result = self.learner.discrete_mle_estimateparams(self.graph_skeleton, data)
                if log:
                    print json.dumps(result.E, indent=2)
                    print json.dumps(result.Vdata, indent=2)
            
            elif self.lib == "pgmpy":
                estimator = BayesianEstimator(self.best_model, data)
                for node in self.best_model.nodes():
                    cpd = estimator.estimate_cpd(node)
                    self.best_model.add_cpds(cpd)
                    if log:
                        print(cpd)
        
        
        def draw_network(self):
            ''' (6) Draws the netork.
            '''
            if self.lib == "libpgm":
                graph = BayesianModel()
                for v in self.graph_skeleton.V:
                    graph.add_node(v)
                for e in self.graph_skeleton.E:
                    graph.add_edge(e[0], e[1])
                pos = nx.spring_layout(graph)
                nx.draw_networkx_nodes(graph, pos, cmap=plt.get_cmap('jet'), node_size = 500)
                nx.draw_networkx_labels(graph, pos, font_size=9)
                nx.draw_networkx_edges(graph, pos)
                plt.show() 
                
            elif self.lib == "pgmpy":
                pos = nx.spring_layout(self.best_model)
                nx.draw_networkx_nodes(self.best_model, pos, cmap=plt.get_cmap('jet'), node_size = 500)
                nx.draw_networkx_labels(self.best_model, pos, font_size=9)
                nx.draw_networkx_edges(self.best_model, pos)
        
        
        
        
        