'''
Created on 24 nov 2017
@author: Alessandro Pozzi, Lorenzo Costantini

This is the main module of the project. 
Running this module will start the processing of one - or all - the file/priority combinations.
You can change some the parameters in this module in order to see how the output varies.
Parameters that can be changed usually have a comment that shows which values can be selected.
'''
from Network_handler import Network_handler
from Pre_network_handler import Pre_network_handler
from DataError import DataError
from General_handler import General_handler

priority = ('L0', 'L1', 'L2', 'L3') # Hard coded priority, do NOT change
select_priority = 'L0' # 'L0', 'L1', 'L2', 'L3' -- ONLY FOR MODE=="ONE"
file_selection = 7  # 1 to 7 -->  ("EMC0019", "EHS60BE", "ES115H", "ESS184", "EXS48X", "EXS1062X", "CUSTOM")
                    # use "CUSTOM" (file number 7) in mode="one" to generate a custom network from expandDevice2 (SET PRIORITY L0)
mode = "one" #one, all  | "one" to do the single file-priority selected above; 
                        # "all" to do all the possible files and priorities

def preprocess_network(select_priority, file_selection, gh, log):
    
    pre_network_handler = Pre_network_handler(gh)
    
    # 1) PROCESS FILES
    file_suffix = "_7net-nooverlaps-yessingledup-emc001"
    pre_network_handler.process_files(select_priority, file_selection, file_suffix, log)
    
    # 2) SELECT VARIABLES
    var_type = "frequency" #occurrences, frequency
    support = 0.4
    MIN = 7
    MAX = 8
    pre_network_handler.select_variables(var_type, MIN, MAX, support, log)
    
    # 3) BUILD DATA
    training_instances="all_events" #all_events, all_events_priority
    pre_network_handler.build_data(training_instances, log)
    
    # RETURN the pre_network_handler object
    return pre_network_handler

def create_network(pnh, gh, log):

    network_handler = Network_handler(pnh, gh)
    
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
    
    # 7) DRAW THE NETWORK
    label = "double" # none, single, double
    location_choice = True # True, False
    location = 1 # 0, 1, 2 (i.e. H0, H1, H2)
    network_handler.draw_network(label, location_choice, location, log)
    
    # 8 ) DATA INFO
    selection = [1, 4] #Put in the list what you want to show
    # 1: Device frequency and occurrences
    # 2: Edges of the network
    # 3: Markov Network
    # 4: Inference network
    network_handler.data_info(selection, log)
    

    
''' The main script to create the BNs '''
def run_script(mode):
    
    if mode == "one":
        try:
            gh = General_handler()
            pnh = preprocess_network(select_priority, file_selection, gh, log = True)
            print("Location search started...")
            gh.getLocations() # LOCATION SEARCH
            print("Location search completed.")
            create_network(pnh, gh, log = False)
        except DataError as e:
            print(e.args[0])
    elif mode == "all":
        print("RUN started...")
        gh = General_handler()
        pnhs = [] # LIST OF THE PRE-NETWORK HANDLERS
        i = 1
        while i <= 6:
            for p in priority:
                print("File " + str(i) + " with priority " + p + ":")
                try:
                    pnh = preprocess_network(p, i, gh, log = False)
                    pnhs.append(pnh)
                except DataError as e:
                    print(e.args[0])
                else:
                    print("Preprocessing completed.")
            i = i + 1
        print("Location search started...")
        gh.getLocations() # LOCATION SEARCH
        print("Location search completed.") 
        
        for pnh in pnhs:
            print("Network creation for file " + pnh.get_device() + 
                  " and priority " + pnh.get_priority() + " started...")
            try:
                create_network(pnh, gh, log = False)
            except DataError as e:
                print(e.args[0])
            else:
                print("Network creation completed.")
        
        gh.save_to_file()        
    print("RUN completed")
    
    
run_script(mode)

