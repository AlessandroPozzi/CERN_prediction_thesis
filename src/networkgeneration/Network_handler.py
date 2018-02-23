
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
from Log_extractor import Log_extractor
from DataError import DataError
from File_writer import File_writer
from DataError import DataError
import pydot
import graphviz as gv
import os
import config

class Network_handler:
    '''
    Handles creation and usage of the probabilistic network over CERN's data.
    Can deal only with a SINGLE file-priority combination.
    Note that the methods of this class have numbers and must be called in order.
    '''

    def __init__(self, pnh, gh):
        '''
        Constructor
        '''
        extractor = pnh.get_data_extractor()
        self.best_model = BayesianModel()
        self.training_instances = ""
        self.device_considered = pnh.get_device()
        self.device_considered_realName = pnh.device_considered_realName
        self.priority_considered = pnh.get_priority()
        self.markov = MarkovModel()
        self.general_handler = gh
        self.devicesColumnDict = extractor.devicesColumnDict
        self.variables_names = extractor.get_variable_names()
        self.rankedDevices = extractor.get_ranked_devices()
        self.data = pnh.get_dataframe()
        self.file_writer = pnh.get_file_writer()
        self.file_suffix = pnh.get_file_suffix()
        self.devicesColumnDict = pnh.get_column_analysis()
        self.occurrences = pnh.get_occurrences()
        
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
        
        for edge in self.best_model.edges_iter():
            self.file_writer.write_txt(str(edge))
        
        self.log("Method used for structural learning: " + method, log)
        #self.log("Training instances skipped: " + str(self.extractor.get_skipped_lines()), log)
        self.log("Search terminated", log)
    
    
    def estimate_parameters(self, log=True):
        ''' (5)
        Estimates the parameters of the found network
        '''
        estimator = BayesianEstimator(self.best_model, self.data)
        self.file_writer.write_txt("Number of nodes: " + str(len(self.variables_names)))
        self.file_writer.write_txt("Complete list: " + str(self.variables_names))
        
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
                        
                                            
    def draw_network(self, label_choice, location_choice, info_choice, variance, refDevice):
        ''' (7) 
        Draws the bayesian network.
        ----
        location_choice = True iff we want to show the location of devices in the graph.
        label_choice = "single" if we want to show single label, "double" for double label of arcs
        '''
        
        if location_choice and info_choice:
            DataError("location_choice and info_choice can't be both True")
            return
        bn_graph = gv.Digraph(format = "png")
        bn_graph.graph_attr['overlap'] = "false"
        devices = self.best_model.nodes()
        self.edgeLabels = []
        
        # Create subgraph for the locations
        if location_choice:
            device_location = dict()
            device_locationH1 = dict()
            
            #For H0 and H1:
            for d in devices:
                allDevicesLocations = self.general_handler.get_device_locations()
                device_location[d] = allDevicesLocations[d][0] #H0
                device_locationH1[d] = allDevicesLocations[d][1] #H1
            if refDevice:
                ref = self.device_considered_realName
                device_location[ref] = allDevicesLocations[ref][0] #H0
                device_locationH1[ref] = allDevicesLocations[ref][1] #H1
            location_color = self.assign_color(device_location)
            location_colorH1 = self.assign_color(device_locationH1)
            
            # Creating the subgraphs, one for each location:
            loc_subgraphs = dict()
            for loc in location_color:
                name = "cluster_" + loc #IL NOME DEL SOTTOGRAFO DEVE INIZIARE PER "cluster_" 
                loc_subgraphs[loc] = gv.Digraph(name) 
                loc_subgraphs[loc].graph_attr['label'] = loc #Label with name to be visualized in the image
                        
        # Create subgraphs for occurrences, average and standard deviation:
        if info_choice:
            info_subgraphs = dict()
            # there's one subgraph for each node:
            id = 0
            for d in devices:
                name = "cluster_" + str(id) #IL NOME DEL SOTTOGRAFO DEVE INIZIARE "cluster_" 
                id += 1
                info_subgraphs[d] = gv.Digraph(name)
                label = "Tot: " + str(round(self.occurrences[d], 2)) + " | "
                label = label + "Avg: " + str(round(self.devicesColumnDict[d].msAverage / 1000, 2)) + "s\n"
                label = label + "St.Dev.: " + str(round(self.devicesColumnDict[d].msStandDev / 1000, 2)) + "s"
                info_subgraphs[d].graph_attr['label'] = label #Label with name to be visualized in the image
                #info_subgraphs[d].graph_attr['penwidth'] = 0
    
        # Create nodes
        for node in self.best_model.nodes():
            if location_choice:
                locationH0 = device_location[node]
                locationH1 = device_locationH1[node]
                loc_subgraphs[locationH0].node(node, style='filled',fillcolor=location_colorH1[locationH1]) #add the node to the right subgraph
                #loc_subgraphs[locationH0].node(node) #USE THIS TO ADD ONLY H0
            elif info_choice:
                info_subgraphs[node].node(node)
            else: #add the node directly to the graph
                bn_graph.node(node)
                
        # Reference device
        if refDevice and location_choice:
            ref = self.device_considered_realName
            locationH0 = device_location[ref]
            locationH1 = device_locationH1[ref]
            loc_subgraphs[locationH0].node(ref, style='filled',fillcolor=location_colorH1[locationH1])
        else:
            ref = self.device_considered_realName
            bn_graph.node(ref)
            
        # Add all subgraphs in the final graph:
        if location_choice:
            for loc in loc_subgraphs:
                bn_graph.subgraph(loc_subgraphs[loc])
        elif info_choice:
            for dev in info_subgraphs:
                bn_graph.subgraph(info_subgraphs[dev])
                
        # Legend for color:
        if location_choice:
            legend = ""
            for loc in location_colorH1:
                legend = legend + location_colorH1[loc] + " --> " + loc + "\n"
            bn_graph.node(legend, shape="box")
        
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
                
            # Save a list of tuples with edge-label data (will be used in post-processing):
            self.edgeLabels.append((edge[0], edge[1], value, value_inv))
            
            if value >= 0.75:
                bn_graph.edge(edge[0], edge[1], color = "red", label = label)
            else:
                bn_graph.edge(edge[0], edge[1], color = "black", label = label)
                
        if refDevice:
            for node in self.best_model.nodes():
                for tpl in self.rankedDevices:
                    if tpl[0] == node:
                        supp = tpl[1]
                        break
                bn_graph.edge(self.device_considered_realName, node, color = "blue", label = str(supp))
                
        
        # Save the .png graph
        if self.device_considered=="CUSTOM":
            imgPath = '../../output/CUSTOM' + self.file_suffix
        else:
            if location_choice:
                locat = "_H0H1"
            else:
                locat = ""
            if variance:
                var = "Var"
            else:
                var = ""
            if config.unitePriorities:
                prior = "all_prior"
            else:
                prior = self.priority_considered
            imgPath = '../../output/' + self.device_considered + '_' + prior + locat + var 
        bn_graph.render(imgPath)
        os.remove(imgPath) #remove the source code generated by graphviz


    def data_info(self, selection, log):
        ''' (9) Prints or logs some extra information about the data or the network
        '''
        # 1 - DEVICE FREQUENCY AND OCCURRENCES
        if 1 in selection:
            self.file_writer.write_txt("Device ranking (max 30 devices are visualized)", newline = True)
            i = 1
            for dr in self.rankedDevices:
                self.file_writer.write_txt(dr[0] + "             \t" + str(dr[1]) + 
                                  "\t" + str(dr[2]) + "\t" + str(dr[3]) + 
                                  "            \t" + str(dr[4]))
                i = i + 1
                if i == 30:
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
            nice_graph.write_png('../../output/' + self.device_considered 
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
                    if prob1 >= 0.75 and (prob1 - prob2) <= 0.40: #add direct arc from node1 to node2
                        ls = [node1, node2]
                        self.fix_node_presence(ls, nice_graph)
                        double_label = str(prob1) + "|" + str(prob2)
                        nice_graph.add_edge(pydot.Edge(node1, node2, color = "red", label = double_label))
                    elif prob2 >= 0.75 and (prob2 - prob1) <= 0.40:
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
            
            if self.device_considered=="CUSTOM":
                imgPath = '../../output/CUSTOM' + self.file_suffix
                nice_graph.write_png(imgPath + "-inference_network.png")
            else:     
                nice_graph.write_png('../../output/' + self.device_considered 
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
                self.file_writer.write_txt("Node " + str(nodeX) + " has no edges: it has been eliminated.")
                self.best_model.remove_node(nodeX)
        if self.best_model.nodes() == []:
            raise DataError("No nodes left in this file-priority combination.")
                
    def assign_color(self, device_location):
        '''
        Returns a dictionary with the location as key and the assigned colour as value (WORKS WITH MAX 10 DIFFERENT LOCATIONS)
        '''
        system_color = ['Blue', 'Green', 'Red', 'Purple', 'Yellow', 'Red', 'Grey', 'Aqua', 'Light Red', 'Light Blue', 'Light Green']
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
    
    
        