'''
Created on 24 nov 2017

@author: Alessandro Corsair
'''
from Network_handler import Network_handler

priority = ('L0', 'L1', 'L2', 'L3')
select_priority = ['L1'] # 'L0', 'L1', 'L2', 'L3'
file_selection = [3] # 1 to 6 -->  ("EMC0019", "EHS60BE", "ES115H", "ESS184", "EXS48X", "EXS1062X")

mode = "one" #one, all   | "one" to do the single file-priority above; "all" to do all files and priorities


def create_network(select_priority, file_selection, log):

    network_handler = Network_handler()
    
    # 1) PROCESS FILES
    network_handler.process_files(select_priority, file_selection, log)
    
    # 2) SELECT VARIABLES
    var_type = "all_frequency"    #all_count, file_name, all_frequency
    var_num = 6
    support = 0.4
    filter = "support_bound" #counting, support, support_bound
    extra_var = "none"  #none, causes
    network_handler.select_variables(var_type, var_num,  support, filter, extra_var, log)
    
    # 3) BUILD DATA
    library = "pgmpy"               #pgmpy, libpgm, pyBN, pomegranate
    training_instances="all_events" #all_events, all_events_with_causes, all_events_priority, support
    priority_node = False
    network_handler.build_data(library, training_instances, priority_node, log)
    
    # 4) LEARN THE STRUCTURE
    network = "bayesian" #bayesian, markov
    method = "scoring_approx"      #scoring_approx, constraint, scoring_exhaustive
    scoring_method = "K2"  #bic, K2, bdeu
    prior = "none"          #none, priority, trigger
    network_handler.learn_structure(network, method, scoring_method, prior, log)
    
    # 5) ESTIMATE THE PARAMETERS
    network_handler.estimate_parameters(log)
    
    # 6) INFERENCE
    mode = "auto" #manual, auto    | with "auto" inference is done on all variables by setting the parents to 1
    variables = ["EMD1A*9"] #list of target variables (for manual mode)
    evidence = dict()
    evidence["EMD3A*9"] = 1 #for manual mode, set the evidence to 1 in the dictionary
    network_handler.inference(variables, evidence, mode, log)
    
    #7 ) DATA INFO
    #network_handler.data_info()
    
    # 8) DRAW THE NETWORK
    network_handler.draw_network()
    
    
def run_script(mode):
    
    if mode == "one":
        create_network(select_priority, file_selection, log = True)
    elif mode == "all":
        print("RUN started...")
        i = 1
        while i <= 6:
            for p in priority:
                print("File " + str(i) + " with priority " + p + " started...")
                create_network([p], [i], log = False)
                print("Processing completed...")
            i = i + 1
                
    print("RUN completed")
    
    
run_script(mode)
