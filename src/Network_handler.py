'''
Created on 23 nov 2017

@author: Alessandro Pozzi, Lorenzo Costantini
'''

import pydot
from Data_extractor import Data_extractor
from pgmpy.estimators import HillClimbSearch, BicScore, BayesianEstimator, K2Score, BdeuScore
from pgmpy.models import BayesianModel
from pgmpy.estimators import ConstraintBasedEstimator
from pgmpy.inference import VariableElimination
from pgmpy.estimators.ExhaustiveSearch import ExhaustiveSearch
from graphviz import Digraph
from DataError import DataError
from pgmpy.factors.discrete import CPD
from pgmpy.models import MarkovModel
from pgmpy.inference import BeliefPropagation
from pgmpy.factors.discrete import DiscreteFactor
from pgmpy.inference import Mplp


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
        self.true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
        self.extractor = Data_extractor()
        self.best_model = BayesianModel()
        self.data = []
        self.training_instances = ""
        self.device_considered = "" #works only when a single device is selected - used for graph
        self.priority_considered = "" #works only when a single priority is selected - used for graph
        self.method = "" #used for graph
        self.markov = MarkovModel()


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
            raise DataError("Priority or file not chosen. Exiting now.")
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
    
    def select_variables(self, var_type, var_num = 6, support = 0.33, filter = "support", extra_var = "none", log = True):
        ''' (2)
        Method that selects the variables to be used in the network.
        -----------------
        Parameters:
        var_type  : The origin of the variables to be considered. Accepted values:
           -> all_count       - If we consider the devices from the complete event list
           -> all_frequency   - If we consider the frequency of the devices in the complete event list
        var_num   : How many variables to take.
        support   : Minimum support to consider the device in the final Bayesian Network
        filter    : Method for filtering variables, i.e. some variables will not be taken according to this filter
           -> support        - takes only variables above a minimum support
           -> counting       - just takes the number of variables indicated. Equivalent to "nofilter"
           -> support_bound  - as "support", but always takes a number of vars above a MIN and below a MAX
        extra_var : Adds extra variables.
           -> none      - Default value. No extra variable is taken.
           -> causes    - To add the 6 extra variables corresponding to the 6 file devices.
        log       : "True" if you want to print debug information in the console    
        '''
        if self.extractor.nodata():
            raise DataError("no data in this file - priority")
            
        if var_type == "all_count":
            ordered_list = self.extractor.count_occurrences_variables()
            if filter == "counting":
                self.extractor.take_n_variables_count(var_num)
            elif filter == "support":
                self.extractor.take_n_variables_support(var_type, support)
            if log:
                print(ordered_list)
                
        elif var_type == "all_frequency":
            ordered_list = self.extractor.frequency_occurences_variables()
            if filter == "counting":
                self.extractor.take_n_variables_count(var_num)
            elif filter == "support":
                self.extractor.take_n_variables_support(var_type, support)
            elif filter == "support_bound":
                self.extractor.take_n_variables_support(var_type, support, filter)
            if log:
                print(ordered_list)
                
        if extra_var == "causes":
            causes = ['trigger_EHS60/BE', 'trigger_EMC001*9', 'trigger_ESS11/5H', 'trigger_ESS1*84', 'trigger_EXS4/8X', 'trigger_EXS106/2X']
            self.extractor.add_variable_names(causes)
  
    def build_data(self, training_instances="all_events",  priority_node = False, log=True):
        ''' (3)
        Method that builds the data to be used by the graphical model library.
        -----------------
        Parameters:
        training_instances : 
            support                -- to use duplicated training instances based on the support of the frequent sets
            all_events             -- to generate one training instance per event (in "distinct devices after 5 minutes")
            all_events_with_causes -- like all_events, but also considers the 6 causes variables
            all_events_priority    -- like all_event but instead of using [0, 1] as values for variables, uses the priority related to the event: [0, L0, L1, L2, L3]
        priority_node   : True if you want the prority node, false otherwise.
        log       : "True" if you want to print debug information in the console    
        '''
        
        variables = self.extractor.get_variable_names()
        self.training_instances = training_instances
        self.data = self.extractor.build_dataframe(training_instances, priority_node)
        
        if log:
            print("There are " + str(len(variables)) + " variables in the network: " + " ".join(variables))
            print("Library used: pgmpy")
            print("There are " + str(len(self.data)) + " 'training' instances in the dataframe.")    



        
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
        prior           : Initial condition for the structure
            -> none     - default
            -> priority - Start with edges between priority and the 6 main devices. 
        log             - "True" if you want to print debug information in the console    
        '''
        #Select the scoring method for the local search of the structure
        if scoring_method == "K2":
            scores = K2Score(self.data)
        elif scoring_method == "bic":
            scores = BicScore(self.data)
        elif scoring_method == "bdeu":
            scores = BdeuScore(self.data)
        
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

        # REMOVE all nodes not connected to anything else:
        self.eliminate_isolated_nodes()
            
        if log:
            print("Training instances skipped: " + str(self.extractor.get_skipped_lines()))
            print("Search terminated")
    

    
    def estimate_parameters(self, log=True):
        ''' (5)
        Estimates the parameters of the found network
        '''
        estimator = BayesianEstimator(self.best_model, self.data)
        output_file  = open('../output/' + self.device_considered 
                 + '_' + self.priority_considered + ".txt","w")
        output_file.write("Number of nodes: " + str(len(self.extractor.get_variable_names())) + "\n")
        output_file.write("Complete list: " + " ".join(self.extractor.get_variable_names()))
        output_file.write("\n")
        for node in self.best_model.nodes():
            cpd = estimator.estimate_cpd(node, prior_type='K2')
            self.best_model.add_cpds(cpd)
            if log:
                print(cpd)
            output_file.write(cpd.__str__())
            output_file.write("\n")
            
        output_file.write("\n")
        output_file.write("Device ranking (max 20 devices are visualized)")
        i = 1
        for dr in self.extractor.get_ranked_devices():
            output_file.write("\n")
            output_file.write(dr[0] + "          -  " + str(dr[1]))
            i = i + 1
            if i == 20:
                break
        output_file.close()
        
        self.markov = self.best_model.to_markov_model()
        
    def inference(self, variables, evidence, mode = "auto", log = True):
            
            inference = VariableElimination(self.best_model)
            #inference = BeliefPropagation(self.markov)
            #inference = Mplp(self.best_model)
            if log:
                print("------------------- INFERENCE ------------------------")
            with open ('../output/' + self.device_considered 
                               + '_' + self.priority_considered +
                               '.txt', 'a') as in_file:
                        in_file.write("\n")
                        in_file.write("------------------- INFERENCE ------------------------")
                        in_file.write("\n")
                        in_file.write("(with parents all set to value 1)")
                        in_file.write("\n")
            
            if mode == "auto":
                if log:
                    print("          (with parents all set to value 1)")
                for node in self.best_model.nodes():
                    variables = [node]
                    parents = self.best_model.get_parents(node)
                    evidence = dict()
                    for p in parents:
                        evidence[p] = 1
                    phi_query = inference.query(variables, evidence)
                    for key in phi_query:
                        with open ('../output/' + self.device_considered 
                                   + '_' + self.priority_considered +
                                   '.txt', 'a') as in_file:
                            in_file.write(str(phi_query[key]))
                            in_file.write("\n")
                        if log:
                            print(phi_query[key])
            
            elif mode == "manual":
                phi_query = inference.query(variables, evidence)
                for key in phi_query:
                    if log:
                        print(phi_query[key])
            
                
                '''
                variables = ["ETZ11/5H"]
                evidence = dict()
                evidence["EAS11/8H"] = 1
                evidence["ECC01/5DX"] = 0
                evidence["ESS11/6A"] = 0
                evidence["ESS11/P18"] = 0
                print("MAP QUERY: " + "\n")
                map_query = inference.map_query(variables, evidence)
                print(map_query)
                '''
                        
                                            
    def draw_network(self, log = False):
        ''' (6) Draws the network.
        '''
        
                
        nice_graph = pydot.Dot(graph_type='digraph')
        for node in self.best_model.nodes():
            node_pydot = pydot.Node(node)
            nice_graph.add_node(node_pydot)
        with open ('../output/' + self.device_considered 
                                 + '_' + self.priority_considered +
                                 '.txt', 'a') as in_file:
            in_file.write("\n")
            in_file.write("Edges of the network: ")
            in_file.write("\n")
        for edge in self.best_model.edges_iter():
            with open ('../output/' + self.device_considered 
                                 + '_' + self.priority_considered +
                                 '.txt', 'a') as in_file:
                in_file.write(str(edge))
                in_file.write("\n")
            inference = VariableElimination(self.best_model)
            variables = [edge[1]]
            evidence = dict()
            evidence[edge[0]] = 1
            phi_query = inference.query(variables, evidence)
            value = phi_query[edge[1]].values[1]
            value = round(value, 2)
            
            variables = [edge[0]]
            evidence = dict()
            evidence[edge[1]] = 1
            phi_query = inference.query(variables, evidence)
            value_inv = phi_query[edge[0]].values[1]
            value_inv = round(value_inv, 2)
            
            #small_label = str(value)
            big_label = str(value) + "|" + str(value_inv)
            #, label = big_label
            if value >= 0.75:
                edge_pydot = pydot.Edge(edge[0], edge[1], color = "red", label = big_label)#red
            else:
                edge_pydot = pydot.Edge(edge[0], edge[1], color = "black", label = big_label)

            nice_graph.add_edge(edge_pydot)
        nice_graph.write_png('../output/' + self.device_considered 
                                 + '_' + self.priority_considered + '.png')
            
        #MARKOV

        nice_graph = pydot.Dot(graph_type='graph')
        for node in self.markov.nodes():
            node_pydot = pydot.Node(node)
            nice_graph.add_node(node_pydot)
        for edge in self.markov.edges():
            edge_pydot = pydot.Edge(edge[0], edge[1], color = "black")
            nice_graph.add_edge(edge_pydot)
        nice_graph.write_png('../output/' + self.device_considered 
                                 + '_' + self.priority_considered + '-markov.png')
        
        with open ('../output/' + self.device_considered 
             + '_' + self.priority_considered +
             '.txt', 'a') as in_file:
            in_file.write("MARKOV NETWORK FACTORS:")
            in_file.write("\n")
            for factor in self.markov.factors:
                if log:
                    print("MARKOV---------------------------------------")
                    print(factor)
                in_file.write(factor.__str__())
                in_file.write("\n")
                
                
        # INFERENCE NETWORK
        nice_graph = pydot.Dot(graph_type='digraph')
        nodes = self.best_model.nodes()
        inference = VariableElimination(self.best_model)
        for node1 in nodes:
            pos = nodes.index(node1) + 1
            for i in range(pos, len(nodes)):
                node2 = nodes[i]
                variables = [node2]
                evidence = dict()
                evidence[node1] = 1
                phi_query = inference.query(variables, evidence)
                prob1 = phi_query[node2].values[1] #probability of direct activation (inference from node1=1 to node2)
                variables = [node1]
                evidence = dict()
                evidence[node2] = 1
                phi_query = inference.query(variables, evidence)
                prob2 = phi_query[node1].values[1] #probability of inverse activation (inference from node2=1 to node1)
                prob1 = round(prob1, 2)
                prob2 = round(prob2, 2)
                if prob1 >= 0.90 and prob2 <= 0.50: #add direct arc from node1 to node2
                    ls = [node1, node2]
                    self.fix_node_presence(ls, nice_graph)
                    nice_graph.add_edge(pydot.Edge(node1, node2, color = "red", label = str(prob1)))
                elif prob2 >= 0.90 and prob1 <= 0.50:
                    ls = [node1, node2]
                    self.fix_node_presence(ls, nice_graph)
                    nice_graph.add_edge(pydot.Edge(node2, node1, color = "red", label = str(prob2)))
                elif prob1 >= 0.75 and prob2 >= 0.75:
                    ls = [node1, node2]
                    self.fix_node_presence(ls, nice_graph)
                    nice_graph.add_edge(pydot.Edge(node2, node1, color = "orange", label = str(prob2)))
                    nice_graph.add_edge(pydot.Edge(node1, node2, color = "orange", label = str(prob1)))
                elif prob1 >= 0.65 and prob2 >= 0.65 and prob1 <= 0.75 and prob2 <= 0.75:
                    ls = [node1, node2]
                    self.fix_node_presence(ls, nice_graph)
                    nice_graph.add_edge(pydot.Edge(node2, node1, color = "black", label = str(prob2)))
                    nice_graph.add_edge(pydot.Edge(node1, node2, color = "black", label = str(prob1)))
                
        nice_graph.write_png('../output/' + self.device_considered 
                                 + '_' + self.priority_considered + '-inference_network.png')            
                    

    def fix_node_presence(self, nodes, pydot_graph):
        ''' Adds the list of nodes to the graph, if they are not already present '''
        for node in nodes:
            if node not in pydot_graph.get_nodes():
                pydot_graph.add_node(pydot.Node(node))
                
    def eliminate_isolated_nodes(self):
        '''
        If a node doesn't have any incoming or outgoing edge, it is eliminated from the graph
        '''
        for nodeX in self.best_model.nodes():
            tup = [item for item in self.best_model.edges() if nodeX in item]
            if not tup:
                self.best_model.remove_node(nodeX)

    def data_info(self):
        
        '''
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
        '''
        
    
    
        