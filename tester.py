'''
Created on 23 nov 2017

@author: Alessandro Corsair
'''
'''
Created on 13 nov 2017

@author: Alessandro Corsair
'''
import networkx as nx
from Data_extractor import Data_extractor
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

file_names = ["EHS60BE","EMC0019", "ES115H","ESS184","EXS48X","EXS1062X"]
true_device_names = ['EHS60/BE', 'EMC001*9', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
#file_names = ["EHS60BE", "EMC0019", "ES115H"]
#true_device_names = ['EHS60/BE','EMC001*9', 'ESS11/5H']


''' EXTRACT DATA FROM TXT FILES'''
extractor = Data_extractor()
ignore_priority = [] #Put in this list the priority levels to ignore completely
for name, true_name in zip(file_names, true_device_names):
    extractor.extract(name, true_name, ignore_priority)



''' PREPROCESSING OF VARIABLES '''

''' 1) Consider only the 6 devices in the BN 
extractor.reset_variable_names(true_device_names) #TEST: consider only the 6 most frequent devices
print("We consider only 6 devices") '''

''' 2) Count occurrences of frequent devices and takes only the n most frequent
ordered_list = extractor.count_occurrences_variables()
print(ordered_list)
extractor.take_n_variables(13)
'''
''' 3) Count occurrences of ALL devices and selects only the n most frequent '''
ordered_list = extractor.count_occurrences_all_devices()
extractor.take_n_variables(9)

''' 4) Adds 6 extra variables as "causes" (compatible with the previous points)
causes = ['trigger_EHS60/BE', 'trigger_EMC001*9', 'trigger_ESS11/5H', 'trigger_ESS1*84', 'trigger_EXS4/8X', 'trigger_EXS106/2X']
extractor.add_variable_names(causes)
'''


#print(ordered_list)

''' BUILDING THE DATAFRAME
variables = extractor.get_variable_names()
print("There are " + str(len(variables)) + " variables in the network: " + " ".join(variables)) '''
''' 1) Pandas dataframe for pgmpy library 
data = extractor.build_dataframe(training_instances="all_events")
print("There are " + str(len(data)) + " 'training' instances in the dataframe.")
'''
''' 2) Dictionary for libpgm library '''
data = extractor.build_libpgm_data(training_instances="all_events")



''' ONLY FOR LIBPGM: EVERYTHING IS DONE HERE '''
learner = PGMLearner()
graph_skeleton = learner.discrete_constraint_estimatestruct(data)
result = learner.discrete_mle_estimateparams(graph_skeleton, data)
#result = learner.discrete_estimatebn(data)
print json.dumps(result.E, indent=2)
print json.dumps(result.Vdata, indent=2)
graph = BayesianModel()
for v in graph_skeleton.V:
    graph.add_node(v)
for e in graph_skeleton.E:
    graph.add_edge(e[0], e[1])
    
pos = nx.spring_layout(graph)
nx.draw_networkx_nodes(graph, pos, cmap=plt.get_cmap('jet'), node_size = 500)
nx.draw_networkx_labels(graph, pos, font_size=9)
nx.draw_networkx_edges(graph, pos)

plt.show() #keep this at the end


''' SELECT METHOD FOR STRUCTURE LEARNING '''
''''1) Scoring methods '''
#scoring_method = BicScore(data)
scoring_method = K2Score(data)
print("Search for best approximated structure started...")
est = HillClimbSearch(data, scoring_method)


''' 2) Exhaustive search 
search = ExhaustiveSearch(data)
for score, model in search.all_scores():
    print("{0}    {1}".format(score, model.edges))
'''

''' STRUCTURE LEARNING '''
''' 1) Start with edges between priority and the 6 main devices 
start_model = BayesianModel()
start_model.add_nodes_from(extractor.get_variable_names())
for d in true_device_names:
    if d in extractor.get_variable_names():
        start_model.add_edge('priority', d)
best_model = est.estimate(start=start_model)
'''
''' 2) Start with no edges '''
best_model = est.estimate()

print("Search terminated")



''' DRAWING THE NETWORK '''
pos = nx.spring_layout(best_model)
nx.draw_networkx_nodes(best_model, pos, cmap=plt.get_cmap('jet'), node_size = 500)
nx.draw_networkx_labels(best_model, pos, font_size=9)
nx.draw_networkx_edges(best_model, pos)



''' PARAMETER ESTIMATION '''
estimator = BayesianEstimator(best_model, data)
for node in best_model.nodes():
    cpd = estimator.estimate_cpd(node)
    best_model.add_cpds(cpd)
    print(cpd)
#cpd_C = estimator.estimate_cpd('AUTO-TRANSFERT', prior_type="dirichlet", pseudo_counts=[1, 2])
print("CPDs added to the model.")



''' INFERENCE 
print("Starting inference...")
inference = VariableElimination(best_model)
variables = ['AUTO-TRANSFERT']
evidence = {'priority':'L0', 'EMC001*9': '1'}
phi_query = inference.max_marginal(variables, evidence)
print(phi_query)
'''

plt.show() #keep this at the end

