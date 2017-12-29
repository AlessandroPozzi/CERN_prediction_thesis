'''
Created on 23 nov 2017

@author: Alessandro Pozzi, Lorenzo Costantini
'''
from pgmpy.estimators import HillClimbSearch, BicScore, BayesianEstimator, K2Score, BdeuScore
from pgmpy.models import BayesianModel
from pgmpy.estimators import ConstraintBasedEstimator
from pgmpy.inference import VariableElimination
from pgmpy.estimators.ExhaustiveSearch import ExhaustiveSearch
from pgmpy.models import MarkovModel
from Data_extractor import Data_extractor
from DataError import DataError
from File_writer import File_writer
from Log_extractor import Log_extractor
import pydot


class Network_handler:
    '''
    Handles creation and usage of the probabilistic network over CERN's data.
    Can deal only with a SINGLE file-priority combination.
    Note that the methods of this class have numbers and must be called in order.
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
        self.device_considered = "" 
        self.priority_considered = "" 
        self.markov = MarkovModel()

    def process_files(self, select_priority, file_selection, log = True):
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
        self.extractor.extract(self.file_names[num-1], self.true_device_names[num-1], select_priority)
        self.device_considered = self.file_names[num-1]
        self.priority_considered = select_priority
        
        self.file_writer = File_writer(self.device_considered, self.priority_considered)
        self.log("Priority level considered: " + select_priority, log)
        self.log("File considered: " + str(self.file_names[num-1]), log)
        self.log("Text files data extraction completed.", log)
    
    def select_variables(self, var_type, MIN, MAX, support, log = True):
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
            raise DataError("no data in this file - priority")
            
        self.extractor.prepare_candidates() #computes occurrences and frequency of the devices
        self.extractor.select_candidates(var_type, support, MIN, MAX)
        
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
        priority_node   : True if you want the prority node, false otherwise.
        log       : "True" if you want to print debug information in the console    
        '''
        
        variables = self.extractor.get_variable_names()
        self.training_instances = training_instances
        self.data = self.extractor.build_dataframe(training_instances)
        
        self.log("There are " + str(len(variables)) + " variables in the network: " + " ".join(variables), log)
        self.log("Library used: pgmpy", log)
        self.log("There are " + str(len(self.data)) + " 'training' instances in the dataframe.", log)    



        
    def learn_structure(self, method, scoring_method, log = True):
        ''' (4)
        Method that builds the structure of the data
        -----------------
        Parameters:
        method          : The technique used to search for the structure
            -> scoring_approx     - To use an approximated search with scoring method
            -> scoring_exhaustive - To use an exhaustive search with scoring method
            -> constraint         - To use the constraint based technique
        scoring_method : K2, bic, bdeu
        log             - "True" if you want to print debug information in the console    
        '''
        
        #Select the scoring method for the local search of the structure
        if scoring_method == "K2":
            scores = K2Score(self.data)
        elif scoring_method == "bic":
            scores = BicScore(self.data)
        elif scoring_method == "bdeu":
            scores = BdeuScore(self.data)
        
        #Select the actual method
        if method == "scoring_approx":
            est = HillClimbSearch(self.data, scores)
        elif method == "scoring_exhaustive":
            est = ExhaustiveSearch(self.data, scores)
        elif method == "constraint":
            est = ConstraintBasedEstimator(self.data)
        
        self.best_model = est.estimate()
        self.eliminate_isolated_nodes() # REMOVE all nodes not connected to anything else
            
        self.log("Method used for structural learning: " + method, log)
        self.log("Training instances skipped: " + str(self.extractor.get_skipped_lines()), log)
        self.log("Search terminated", log)
    
    
    def estimate_parameters(self, log=True):
        ''' (5)
        Estimates the parameters of the found network
        '''
        estimator = BayesianEstimator(self.best_model, self.data)
        self.file_writer.write_txt("Number of nodes: " + str(len(self.extractor.get_variable_names())))
        self.file_writer.write_txt("Complete list: " + " ".join(self.extractor.get_variable_names()))
        
        for node in self.best_model.nodes():
            cpd = estimator.estimate_cpd(node, prior_type='K2')
            self.best_model.add_cpds(cpd)
            self.log(cpd, log)
            self.file_writer.write_txt(cpd.__str__())
            
        
    def inference(self, variables, evidence, mode = "auto", log = True):
        ''' (6)
        Computes the inference over some variables of the network (given some evidence)
        '''
            
        inference = VariableElimination(self.best_model)
        #inference = BeliefPropagation(self.markov)
        #inference = Mplp(self.best_model)
        header = "------------------- INFERENCE ------------------------"
        self.log(header, log)
        self.file_writer.write_txt(header, newline = True)
        self.file_writer.write_txt("(With parents all set to value 1)")

        if mode == "auto":
            self.log("          (with parents all set to value 1)", log)
            for node in self.best_model.nodes():
                variables = [node]
                parents = self.best_model.get_parents(node)
                evidence = dict()
                for p in parents:
                    evidence[p] = 1
                phi_query = inference.query(variables, evidence)
                for key in phi_query:
                    self.file_writer.write_txt(str(phi_query[key]))
                    self.log(phi_query[key], log)
        
        elif mode == "manual":
            phi_query = inference.query(variables, evidence)
            for key in phi_query:
                self.log(phi_query[key], log)
        
            '''
            map_query = inference.map_query(variables, evidence)
            print(map_query)
            '''
                        
                                            
    def draw_network(self, label_choice, color_choice, location, log):
        ''' (7) 
        Draws the bayesian network.
        '''
        nice_graph = pydot.Dot(graph_type='digraph')
        args = []
        
        # Extract color based on the building
        if color_choice:
            log_extractor = Log_extractor()
            self.log("Searching for location of devices...", log)
            devices = self.extractor.get_variable_names()
            device_location = log_extractor.find_location(devices, location)
            self.log("Locations found.", log)
            location_color = self.assign_color(device_location)
            # Logging and saving info
            self.log(device_location, log)
            self.log(location_color, log)
            self.file_writer.write_txt(device_location, newline = True)
            self.file_writer.write_txt(location_color, newline = True)
        
        # Create nodes
        for node in self.best_model.nodes():
            if color_choice:
                color = location_color[device_location[node]]
                node_pydot = pydot.Node(node, style="filled", fillcolor=color)
            else:
                node_pydot = pydot.Node(node)
            nice_graph.add_node(node_pydot)
        
        # Create and color edges
        for edge in self.best_model.edges_iter():
            inference = VariableElimination(self.best_model)
            label = ""
            
            # Inference for first label and color of edges
            variables = [edge[1]]
            evidence = dict()
            evidence[edge[0]] = 1
            phi_query = inference.query(variables, evidence)
            value = phi_query[edge[1]].values[1]
            value = round(value, 2)
                
            if label_choice == "single":
                label = str(value)
            
            if label_choice == "double":
                # Inference for second label
                variables = [edge[0]]
                evidence = dict()
                evidence[edge[1]] = 1
                phi_query = inference.query(variables, evidence)
                value_inv = phi_query[edge[0]].values[1]
                value_inv = round(value_inv, 2)
                label = str(value) + "|" + str(value_inv)
            
            if value >= 0.75:
                edge_pydot = pydot.Edge(edge[0], edge[1], color = "red", label = label)
            else:
                edge_pydot = pydot.Edge(edge[0], edge[1], color = "black", label = label)

            nice_graph.add_edge(edge_pydot)
        
        # Save the .png graph    
        nice_graph.write_png('../output/' + self.device_considered 
                                 + '_' + self.priority_considered + '.png')
            
    

    def data_info(self, selection, log):
        ''' (9) Prints or logs some extra information about the data or the network
        '''
        # 1 - DEVICE FREQUENCY AND OCCURRENCES
        if 1 in selection:
            self.file_writer.write_txt("Device ranking (max 20 devices are visualized)", newline = True)
            i = 1
            for dr in self.extractor.get_ranked_devices():
                self.file_writer.write_txt(dr[0] + "             \t" + str(dr[1]) + 
                                  "\t" + str(dr[2]))
                i = i + 1
                if i == 20:
                    break                    
        
        # 2 - EDGES OF THE NETWORK
        if 2 in selection:
            self.file_writer.write_txt("Edges of the network:", newline = True)
            for edge in self.best_model.edges_iter():
                self.file_writer.write_txt(str(edge))
            
        # 3 - MARKOV NETWORK
        if 3 in selection:
            self.markov = self.best_model.to_markov_model() #create the markov model from the BN
            nice_graph = pydot.Dot(graph_type='graph')
            for node in self.markov.nodes():
                node_pydot = pydot.Node(node)
                nice_graph.add_node(node_pydot)
            for edge in self.markov.edges():
                edge_pydot = pydot.Edge(edge[0], edge[1], color = "black")
                nice_graph.add_edge(edge_pydot)
            nice_graph.write_png('../output/' + self.device_considered 
                                     + '_' + self.priority_considered + '-markov.png')
            
            self.file_writer.write_txt("MARKOV NETWORK FACTORS:", newline = True)
            for factor in self.markov.factors:
                self.log("MARKOV---------------------------------------", log)
                self.log(factor, log)
                self.file_writer.write_txt(factor.__str__())
            
        # 4 - INFERENCE NETWORK
        if 4 in selection:
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
                    if prob1 >= 0.75 and (prob1 - prob2) <= 0.25: #add direct arc from node1 to node2
                        ls = [node1, node2]
                        self.fix_node_presence(ls, nice_graph)
                        double_label = str(prob1) + "|" + str(prob2)
                        nice_graph.add_edge(pydot.Edge(node1, node2, color = "red", label = double_label))
                    elif prob2 >= 0.75 and (prob2 - prob1) <= 0.25:
                        ls = [node1, node2]
                        self.fix_node_presence(ls, nice_graph)
                        double_label = str(prob2) + "|" + str(prob1)
                        nice_graph.add_edge(pydot.Edge(node2, node1, color = "red", label = double_label))
                    elif prob1 >= 0.75 and prob2 >= 0.75:
                        ls = [node1, node2]
                        self.fix_node_presence(ls, nice_graph)
                        if prob1 >= prob2:
                            double_label = str(prob1) + "|" + str(prob2)
                            nice_graph.add_edge(pydot.Edge(node1, node2, color = "orange", label = double_label))
                        else:
                            double_label = str(prob2) + "|" + str(prob1)
                            nice_graph.add_edge(pydot.Edge(node2, node1, color = "orange", label = double_label))
                    elif prob1 >= 0.55 and prob2 >= 0.55:
                        ls = [node1, node2]
                        self.fix_node_presence(ls, nice_graph)
                        if prob1 >= prob2:
                            double_label = str(prob1) + "|" + str(prob2)
                            nice_graph.add_edge(pydot.Edge(node1, node2, color = "black", label = double_label))
                        else:
                            double_label = str(prob2) + "|" + str(prob1)
                            nice_graph.add_edge(pydot.Edge(node2, node1, color = "black", label = double_label))
                    
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
                
    def assign_color(self, device_location):
        '''
        Returns a dictionary with the location as key and the assigned colour as value (WORKS WITH MAX 10 DIFFERENT LOCATIONS)
        '''
        system_color = ['Blue', 'Green', 'Red', 'Purple', 'Yellow', 'Red', 'Grey', 'Light Red', 'Light Blue', 'Light Green']
        location_color = dict() # key = location; value = color
        for dev, loc in device_location.items():
            if loc not in location_color:
                color = system_color[0]
                system_color.remove(color)
                location_color[loc] = color
        return location_color

    def log(self, text, log):
        ''' Prints the text in the console, if the "log" condition is True. '''
        if log:
            print(text)
    
    
        