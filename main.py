'''
Created on 24 nov 2017

@author: Alessandro Corsair
'''
from Network_handler import Network_handler

network_handler = Network_handler()

# 1) PROCESS FILES
ignore_priority = []
files_used = 6
network_handler.process_files(ignore_priority, files_used, log = True)

# 2) SELECT VARIABLES
var_type = "all"    #all, file_name
var_num = 6
extra_var = "none"  #none, causes
network_handler.select_variables(var_type, var_num, extra_var, log = True)

# 3) BUILD DATA
library = "pgmpy"               #pgmpy, libpgm
training_instances="all_events" #all_events, all_events_with_causes, all_events_priority, support
priority_node = False
network_handler.build_data(library, training_instances, priority_node, log = True)

# 4) LEARN THE STRUCTURE
method = "scoring"      #scoring, constraint
scoring_method = "bic"  #bic, K2
prior = "none"          #none, priority
network_handler.learn_structure(method, scoring_method, prior, log = True) 

# 5) ESTIMATE THE PARAMTERS
network_handler.estimate_parameters(log = True)

# 6) DRAW THE NETWORK
network_handler.draw_network()