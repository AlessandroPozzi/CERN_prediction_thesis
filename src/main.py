'''
Created on 24 nov 2017
@author: Alessandro Pozzi, Lorenzo Costantini

This is the main file of the project. 
Running this file will start the processing of one - or all - the file/priority combinations.
You can change some the parameters in this module in order to see how the output varies.
Parameters that can be change usually have a comment that shows which values can be selected.
'''
from Network_handler import Network_handler
from DataError import DataError

priority = ('L0', 'L1', 'L2', 'L3') # Hard coded priority, do NOT change
select_priority = ['L1'] # 'L0', 'L1', 'L2', 'L3' -- ONLY FOR MODE=="ONE"
file_selection = [1] # 1 to 6 -->  ("EMC0019", "EHS60BE", "ES115H", "ESS184", "EXS48X", "EXS1062X")

mode = "all" #one, all  | "one" to do the single file-priority selected above; 
                        # "all" to do all the possible files and priorities


def create_network(select_priority, file_selection, log):

    network_handler = Network_handler()
    
    # 1) PROCESS FILES
    network_handler.process_files(select_priority, file_selection, log)
    
    # 2) SELECT VARIABLES
    var_type = "frequency" #occurrences, frequency
    support = 0.4
    MIN = 4
    MAX = 8
    network_handler.select_variables(var_type, support, MIN, MAX, log)
    
    # 3) BUILD DATA
    training_instances="all_events" #all_events, all_events_priority
    network_handler.build_data(training_instances, log)
    
    # 4) LEARN THE STRUCTURE
    method = "scoring_approx"      #scoring_approx, constraint, scoring_exhaustive
    scoring_method = "K2"  #bic, K2, bdeu
    network_handler.learn_structure(method, scoring_method, log)
    
    # 5) ESTIMATE THE PARAMETERS
    network_handler.estimate_parameters(log)
    
    # 6) INFERENCE
    mode = "auto" #manual, auto    | with "auto" inference is done on all variables by setting the parents to 1
    variables = [""] #list of target variables (for manual mode)
    evidence = dict()
    evidence[""] = 1 #for manual mode, set the evidence to 1 in the dictionary
    network_handler.inference(variables, evidence, mode, log)
    
    #7 ) DATA INFO
    #network_handler.data_info()
    
    # 8) DRAW THE NETWORK
    network_handler.draw_network(log = False)
    
    
def run_script(mode):
    
    if mode == "one":
        create_network(select_priority, file_selection, log = True)
    elif mode == "all":
        print("RUN started...")
        i = 1
        while i <= 6:
            for p in priority:
                print("File " + str(i) + " with priority " + p + " started...")
                try:
                    create_network([p], [i], log = False)
                except DataError as e:
                    print("File skipped: " + e.args[0])
                else:
                    print("Processing completed...")
            i = i + 1
                
    print("RUN completed")
    
    
run_script(mode)
