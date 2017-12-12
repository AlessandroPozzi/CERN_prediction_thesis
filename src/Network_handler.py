'''
Created on 23 nov 2017

@author: Alessandro Corsair
'''

import networkx as nx
import re
import numpy as np

from Data_extractor import Data_extractor
from pgmpy.estimators import HillClimbSearch, BicScore, BayesianEstimator, K2Score
from pgmpy.models import BayesianModel
from pgmpy.estimators import ConstraintBasedEstimator
from pgmpy.inference import VariableElimination
from pgmpy.estimators.ExhaustiveSearch import ExhaustiveSearch
from graphviz import Digraph
import json
import pydot
from libpgm.nodedata import NodeData
from libpgm.graphskeleton import GraphSkeleton
from libpgm.discretebayesiannetwork import DiscreteBayesianNetwork
from libpgm.pgmlearner import PGMLearner
from Data_extractor import Data_extractor
import warnings
import matplotlib.pyplot as plt

from pomegranate import *
#import pomegranate.pomegranate-master.pomegranate.BayesianNetwork
#from testBN import BayesianNetwork 
from networkx.classes import ordered
from numpy.core.defchararray import lower

class Network_handler:
    '''
    Handles creation and usage of the probabilistic network over CERN's data.
    Note that the method of this class have numbers and must be called in order.
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.file_names = ["EMC0019", "EHS60BE", "ES115H","ESS184","EXS48X","EXS1062X"]
        self.true_device_names = ["EMC0019", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
        self.extractor = Data_extractor()
        self.lib = ""
        self.learner = PGMLearner()
        self.graph_skeleton = GraphSkeleton()
        self.best_model = BayesianModel()
        self.bn = BayesianNetwork()
        self.data = []
        self.training_instances = ""
        self.device_considered = "" #works only when a single device is selected - used for graph
        self.priority_considered = "" #works only when a single priority is selected - used for graph
        self.method = "" #used for graph


    def process_files(self, select_priority = [], file_selection = [], log = True):
        '''  (1)
        Method that extracts data from the text files.
        -----------------
        Parameters:
        select_priority -- list of the priority levels to be considered
        files_used      -- the number of files to be used (min 1, max 6)
        log             -- "True" if you want to print debug information in the console
        '''
        if not select_priority or not file_selection:
            print("Priority or file not chosen. Exiting now.")
            return
        else:
            if log:
                print("Priority level considered: " + str(select_priority))
        
        info = ""
        for num in file_selection:
            self.extractor.extract(self.file_names[num-1], self.true_device_names[num-1], select_priority)
            info = self.file_names[num-1] + " " + info
            self.device_considered = self.file_names[num-1] #should only be one -used by graph
        self.priority_considered = select_priority[0] #should only be one -used by graph
        if log:
            print("File considered: " + info)
            print("Text files data extraction completed.")
    
    def select_variables(self, var_type, var_num = 6, extra_var = "none", log = True):
        ''' (2)
        Method that selects the variables to be used in the network.
        -----------------
        Parameters:
        var_type  : The origin of the variables to be considered. Accepted values:
           -> all_count       - If we consider the devices from the complete event list
           -> all_frequency   - If we consider the frequency of the devices in the complete event list
           -> file_name - If we consider only the 6 file devices as variables
        var_num   : How many variables to take.
        extra_var : Adds extra variables.
           -> none      - Default value. No extra variable is taken.
           -> causes    - To add the 6 extra variables corresponding to the 6 file devices.
        log       : "True" if you want to print debug information in the console    
        '''
        if var_type == "all_count":
            ordered_list = self.extractor.count_occurrences_variables()
            self.extractor.take_n_variables(var_num)
            if log:
                print(ordered_list)
                
        elif var_type == "all_frequency":
            ordered_list = self.extractor.frequency_occurences_variables()
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
  
    def build_data(self, library, training_instances="all_events",  priority_node = False, log=True):
        ''' (3)
        Method that builds the data to be used by the graphical model library.
        -----------------
        Parameters:
        library            : The library for which the data is formatted
           -> pgmpy 
           -> libpgm 
        training_instances : (for pgmpy)
            support                -- to use duplicated training instances based on the support of the frequent sets
            all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes")
            all_events_with_causes -- like all_events, but also considers the 6 causes variables
            all_events_priority    -- like all_event but instead of using [0, 1] as values for variables, uses the priority related to the event: [0, L0, L1, L2, L3]
        training_instances : (for libpgm)    
            all_events -- to generate one training instance per event (in "distinct devices after 5 minutes")
        priority_node   : True if you want the prority node, false otherwise.
        log       : "True" if you want to print debug information in the console    
        '''
        self.lib = library
        variables = self.extractor.get_variable_names()
        self.training_instances = training_instances
        if log:
            print("There are " + str(len(variables)) + " variables in the network: " + " ".join(variables))
            print("Library used: " + library)
        
        if library == "pgmpy":
            self.data = self.extractor.build_dataframe(training_instances, priority_node)
            if log:
                print("There are " + str(len(self.data)) + " 'training' instances in the dataframe.")    
                
        elif library == "pomegranate":
            self.data = self.extractor.build_numpy_data(training_instances, priority_node)
            if log:
                print("There are " + str(len(self.data)) + " 'training' instances in the dataset")
        
        elif library == "libpgm":
            self.data = self.extractor.build_libpgm_data(training_instances, priority_node)
            if log:
                print("There are " + str(len(self.data)) + " 'training' instances in the dataset")
            
        elif library == "pyBN":
            self.data = self.extractor.build_numpy_data(training_instances, priority_node)
            
        else:
            print("Wrong library chosen")
        
        
    def learn_structure(self, method, scoring_method, prior = "none", log = True):
        ''' (4)
        Method that builds the structure of the data
        -----------------
        Parameters: (only for pgmpy)
        method          : The technique used to search for the structure
            -> scoring    - To use a scoring method
            -> constraint - To use the constraint based technique
        scoring_method  : (only for scoring method)
            -> K2
            -> bic
        prior           : Initial condition for the structure (only for pgmpy)
            -> none     - default
            -> priority - Start with edges between priority and the 6 main devices. In pomegranate, adds trigger in the constraint network.
            -> trigger  - (for pomegranate) Add trigger in constraint network
        log             - "True" if you want to print debug information in the console    
        '''
        if self.lib == "libpgm":
            self.graph_skeleton = self.learner.discrete_constraint_estimatestruct(self.data, pvalparam=0.9)
            
        elif self.lib == "pomegranate":
            
            alg="chow-liu" #greedy, chow-liu, exact, exact-dp

            if prior == "trigger" and self.training_instances == "all_events_with_causes": #Use the constraint graph
                constraints = nx.DiGraph()
                var_names = self.extractor.get_variable_names()
                upper_layer = tuple()
                lower_layer = tuple()
                i = 0
                for device in var_names:
                    check = re.compile('trigger').findall(device)
                    if check:
                        #upper_layer.append(device) #triggers can only be parents
                        upper_layer = upper_layer + (i,)
                    else:
                        #lower_layer.append(device)
                        lower_layer = lower_layer + (i,)
                    i = i + 1
                
                constraints.add_edge(upper_layer, lower_layer)
                constraints.add_edge(lower_layer, lower_layer)
                self.bn = BayesianNetwork.from_samples(self.data, constraint_graph=constraints, algorithm=alg)

                if log:
                    
                    print("There are: " + str(self.bn.node_count()) + " nodes in the network")
                    print("Constraints added to the network")
                    print(self.bn.structure)
                    '''
                    pos = nx.spring_layout(constraints)
                    nx.draw_networkx_nodes(constraints, pos, cmap=plt.get_cmap('jet'), node_size = 500)
                    nx.draw_networkx_labels(constraints, pos, font_size=9)
                    nx.draw_networkx_edges(constraints, pos)
                    plt.show() 
                    '''
            elif prior == "priority":
                constraints = nx.DiGraph()
                var_names = self.extractor.get_variable_names()
                upper_layer = tuple()
                lower_layer = tuple()
                i = 0
                for device in var_names:
                    if device == "priority":
                        upper_layer = upper_layer + (i,)
                    else:
                        lower_layer = lower_layer + (i,)
                    i = i + 1
                constraints.add_edge(upper_layer, lower_layer)
                constraints.add_edge(lower_layer, lower_layer)
                self.bn = BayesianNetwork.from_samples(self.data, constraint_graph=constraints,  algorithm=alg)
                    
                if log:
                    print("There are: " + str(self.bn.node_count()) + "nodes in the network")
                    print("Constraints added to the network")
                    print(self.bn.structure)
                    
            else:
                self.bn = BayesianNetwork.from_samples(self.data,  algorithm=alg)
            
            
        elif self.lib == "pgmpy":
            if scoring_method == "K2":
                scores = K2Score(self.data)
            elif scoring_method == "bic":
                scores = BicScore(self.data)
            
            if method == "scoring_approx":
                if log:
                    print("Search for best approximated structure started...")
                self.method = "scoring_approx"
                est = HillClimbSearch(self.data, scores)
                
            elif method == "scoring_exhaustive":
                if log:
                    print("Exhaustive search for the best structure started...")
                self.method = "scoring_exhaustive"
                est = ExhaustiveSearch(self.data, scores)
                
            elif method == "constraint":
                if log:
                    print("Constraint based method for finding the structure started...")
                self.method = "constraint"
                est = ConstraintBasedEstimator(self.data)
            
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
            
        elif self.lib == "pyBN":
            if method == "scoring":
                hl = pyBN.learning.structure.score.hill_climbing.hc
                hl(self.data)    
            
        if log:
            print("Training instances skipped: " + str(self.extractor.get_skipped_lines()))
            print("Search terminated")
            
        else:
            print("Error in choosing the library")
            return
            

        
    def estimate_parameters(self, log=True):
        ''' (5)
        Estimates the parameters of the found network
        '''
        if self.lib == "libpgm":
            result = self.learner.discrete_mle_estimateparams(self.graph_skeleton, self.data)
            if log:
                print json.dumps(result.E, indent=2)
                print json.dumps(result.Vdata, indent=2)
            
        elif self.lib == "pomegranate":
            self.bn.fit(self.data)
            if log:
                json_data = self.bn.to_json()
                with open('../output/pomegranate_cpds', 'w') as outfile:
                    json.dump(json_data, outfile)
                #print self.bn.to_json(indent = 4)
        
        elif self.lib == "pgmpy":
            estimator = BayesianEstimator(self.best_model, self.data)
            for node in self.best_model.nodes():
                cpd = estimator.estimate_cpd(node, prior_type='K2')
                self.best_model.add_cpds(cpd)
                if log:
                    print(cpd)
        
    def inference(self):
        
        if self.lib == "pomegranate":
            dictionary = dict()
            for var in self.extractor.get_variable_names():
                dictionary[var] = None
            #add manually evidence
            dictionary['ECE001/BE'] = 1
            dictionary['EXS106/2X'] = 1
            numpy_array = np.array([1, 1, 0, None, None, None])
            print(self.bn.predict_proba(numpy_array))
    
        
    def draw_network(self):
        ''' (6) Draws the network.
        '''
        with warnings.catch_warnings():
            warnings.simplefilter("ignore") #suppress warnings
            
            fig = plt.figure()
            fig.canvas.set_window_title("Library: " + self.lib) 
        
            if self.lib == "libpgm":
                graph = BayesianModel()
                for v in self.graph_skeleton.V:
                    graph.add_node(v)
                for e in self.graph_skeleton.E:
                    graph.add_edge(e[0], e[1])
                #pos = nx.spring_layout(model)
                pos = nx.fruchterman_reingold_layout(graph)
                nx.draw_networkx_nodes(graph, pos, cmap=plt.get_cmap('jet'), node_size = 500)
                nx.draw_networkx_labels(graph, pos, font_size=9)
                nx.draw_networkx_edges(graph, pos)
                plt.show()
                
            elif self.lib == "pomegranate":
                model = BayesianModel()
                var_names = self.extractor.get_variable_names()
                for name in var_names:
                    model.add_node(name)
                structure = self.bn.structure
                for parent_tuple, device_name in zip(structure, var_names):
                    for parent in parent_tuple:
                        model.add_edge(var_names[parent], device_name)
                #pos = nx.spring_layout(model)
                pos = nx.fruchterman_reingold_layout(model)
                nx.draw_networkx_nodes(model, pos, cmap=plt.get_cmap('jet'), node_size = 500)
                nx.draw_networkx_labels(model, pos, font_size=9)
                nx.draw_networkx_edges(model, pos)
                plt.show()
                
            elif self.lib == "pgmpy":
                
                nice_graph = pydot.Dot(graph_type='digraph')
                for node in self.best_model.nodes():
                    node_pydot = pydot.Node(node)
                    nice_graph.add_node(node_pydot)
                for edge in self.best_model.edges_iter():
                    edge_pydot = pydot.Edge(edge[0], edge[1])
                    nice_graph.add_edge(edge_pydot)
                nice_graph.write_png('../output/' + self.device_considered 
                                     + '_' + self.priority_considered + '_' + 
                                     self.lib + "_" + self.method + '.png')

                '''
                pos = nx.spring_layout(self.best_model)
                nx.draw_networkx_nodes(self.best_model, pos, cmap=plt.get_cmap('jet'), node_size = 500)
                nx.draw_networkx_labels(self.best_model, pos, font_size=9)
                nx.draw_networkx_edges(self.best_model, pos, arrow = True)
                '''

            

    def data_info(self):
        
        # 1
        print("Showing now the unique devices in frequent itemset, for each file:")
        ufd = self.extractor.get_unique_frequent_devices_by_file()
        print "{:<15} {:<90}".format('File name','Unique devices in frequent itemsets')
        for k, v in ufd.iteritems():
            print "{:<15} {:<90}".format(k, v)
        
        # 2
        max = 36
        print("Showing now the " + str(max) + " absolutely more frequent devices (based on the relative appearance in each file)")
        fov = self.extractor.frequency_occurences_variables()
        print "{:<15} {:<90}".format('Device name','Frequency')
        i = 0 
        for e in fov: 
            i = i + 1
            if i < max: #visualize max "i" elements
                print "{:<15} {:<90}".format(e[0], e[1])    

    
    
        