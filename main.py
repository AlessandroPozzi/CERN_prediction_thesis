'''
Created on 24 nov 2017

@author: Alessandro Corsair
'''
from Network_handler import Network_handler

network_handler = Network_handler()

# 1)
ignore_priority = []
files_used = 6
network_handler.process_files(ignore_priority, files_used, log = True)

# 2)
var_type = "all"
var_num = 6
extra_var = "none"
network_handler.select_variables(var_type, var_num, extra_var, log = True)

# 3)
library = "libpgm"
training_instances="all_events"
network_handler.build_data(library, training_instances, log = True)

# 4)
method = "scoring"
scoring_method = "K2"
prior = "none"
network_handler.learn_structure(method, scoring_method, prior, log = True)

# 5)
network_handler.estimate_parameters(log = True)

# 6)
network_handler.draw_network()