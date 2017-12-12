'''
Created on 24 nov 2017

@author: Alessandro Corsair
'''
from Network_handler import Network_handler

network_handler = Network_handler()

# 1) PROCESS FILES
select_priority = ['L1'] # 'L0', 'L1', 'L2', 'L3'
file_selection = [1] # 1 to 6 -->  ("EMC0019", "EHS60BE", "ES115H", "ESS184", "EXS48X", "EXS1062X")
network_handler.process_files(select_priority, file_selection, log = True)

# 2) SELECT VARIABLES
var_type = "all_frequency"    #all_count, file_name, all_frequency
var_num = 6
support = 0.5
filter = "support" #counting, support
extra_var = "none"  #none, causes
network_handler.select_variables(var_type, var_num,  support, filter, extra_var, log = True,)

# 3) BUILD DATA
library = "pgmpy"               #pgmpy, libpgm, pyBN, pomegranate
training_instances="all_events" #all_events, all_events_with_causes, all_events_priority, support
priority_node = False
network_handler.build_data(library, training_instances, priority_node, log = True)

# 4) LEARN THE STRUCTURE
method = "scoring_approx"      #scoring_approx, constraint, scoring_exhaustive
scoring_method = "K2"  #bic, K2, bdeu
prior = "none"          #none, priority, trigger
network_handler.learn_structure(method, scoring_method, prior, log = True)

# 5) ESTIMATE THE PARAMETERS
network_handler.estimate_parameters(log = True)

# 6) INFERENCE
network_handler.inference()

#7 ) DATA INFO
#network_handler.data_info()

# 8) DRAW THE NETWORK
network_handler.draw_network()