'''
Created on 24 nov 2017
@author: Alessandro Pozzi, Lorenzo Costantini

This is the main module of the project. 
Running this module will start the processing of one - or all - the file/priority combinations.
You can change some the parameters in this module in order to see how the output varies.
Parameters that can be changed usually have a comment that shows which values can be selected.
'''

from General_handler import General_handler
from DataError import DataError
from Network_handler import Network_handler
from Pre_network_handler import Pre_network_handler
from DatabaseNetworkCorrelator import DatabaseNetworkCorrelator
import config
import os

priority = ('L0', 'L1', 'L2', 'L3') # Hard coded priority, do NOT change
select_priority = config.selectPriority
file_selection = config.file_selection
mode = config.mode

def preprocess_network(select_priority, file_selection, gh, log):
    
    pre_network_handler = Pre_network_handler(gh)
    
    # 1) PROCESS FILES
    if config.EXTRA:
        extra = "_" + config.EXTRA
    else:
        extra = ""
    file_suffix = "_" + config.FILE_SUFFIX + extra
    pre_network_handler.process_files(select_priority, file_selection, file_suffix, log)
    
    # 2) SELECT VARIABLES
    var_type = "occurrences" #occurrences, frequency, variance_only, support_variance, lift, manual
    support = 0.3
    MIN = 4
    MAX = 6
    #manualList = [] # nomi delle variabili da aggiungere, CON doppio trattino (COPPIE)
    manualList = ["ECE001*9--", "ECE001/BE--", "EKD208/6E--", "ECE001/8E--", "EKD203/5E--"]
    #manualList = ["ESS11*13--", "EBS1/28--", "EBS1/22--", "EBS132/2X--"]
    #manualList.append("AUTO-TRANSFERT--")
    #manualList.append("ECC01/5DX--")
    #manualList.append("EMC001*9--")
    #manualList.append("EMD102*43--")
    #manualList.append("EMD1A*9--")
    #manualList.append("EMD2A*9--")
    #manualList.append("EMD3A*9--")
    #manualList.append("EMC700/1E--")
    #manualList.append("ESS316/7E--A08")
    #manualList.append("EBS1/12--A15")
    #manualList.append("EBS1/32--A08")
    #manualList.append("EBS1/12--A30")
    #manualList.append("ESS10/1DX--4C-5")
    #manualList.append("EXS311*80--44-2")

    #manualList.append("EBS1/32--A08")
    pre_network_handler.select_variables(var_type, MIN, MAX, support, log, manualList)
    
    # 3) BUILD DATA
    training_instances="all_events" #all_events, all_events_priority
    pre_network_handler.build_data(training_instances, log)
    
    # *) COLUMNS INFO (state, tag, description)
    pre_network_handler.checkColumnDistribution()
    
    # RETURN the pre_network_handler object
    return pre_network_handler

def create_network(pnh, gh, log):

    network_handler = Network_handler(pnh, gh)
    
    # 4) LEARN THE STRUCTURE
    method = "scoring_approx"      #scoring_approx, constraint, scoring_exhaustive
    scoring_method = "bic"  #bic, K2, bdeu
    network_handler.learn_structure(method, scoring_method, log)
    
    # 5) ESTIMATE THE PARAMETERS
    network_handler.estimate_parameters(log)
    
    # 6) INFERENCE
    mode = "auto" #manual, auto, no    | with "auto" inference is done on all variables by setting the parents to 1
    variables = ["EMD311*9--"] #list of target variables (for manual mode)
    evidence = dict()
    evidence["EMD104*9--"] = 1 #for manual mode, set the evidence to 1 in the dictionary
    #evidence[""] = 1
    #evidence["EMD210*9--"] = 1
    network_handler.inference(variables, evidence, mode, log)
    
    # 7) DRAW THE NETWORK
    label = "double" # none, single, double
    location_choice = False # True, False
    onlyH0 = False
    info_choice = False
    variance_filter = False # True, False
    refDevice = False
    hideNames = False
    network_handler.draw_network(label, location_choice, onlyH0, info_choice, variance_filter, refDevice, hideNames)
    
    # 8) DATA INFO
    selection = [1, 2] #Put in the list what you want to show
    # 1: Device frequency and occurrences
    # 2: Edges of the network
    # 3: Markov Network
    # 4: Inference network
    network_handler.data_info(selection, log)
    
    return network_handler
    
def post_processing(nh, gh, log):
    
    dnc = DatabaseNetworkCorrelator()
    dnc.initAsPostProcessor(nh, gh, log)
    
    # 9) CHECK GENERAL CORRELATIONS
    #dnc.checkGeneralCorrelation()
    
    # 10) TOTAL OCCURRENCES ANALYSIS
    #dnc.totalOccurrencesNetworkAnalysis()
    
    
''' The main script to create the BNs '''
def run_script(mode):
    clearDirectory("../../output/")
    clearDirectory("../../output/columnAnalysis/")
    clearDirectory("../../output/postProcessingAnalysis/")
    clearDirectory("../../output/preProcessingAnalysis/")
    if mode == "one":
        try:
            if config.unitePriorities:
                newPriority = "L0" #everything is united in L0
            else:
                newPriority = config.selectPriority
            gh = General_handler()
            pnh = preprocess_network(newPriority, file_selection, gh, log = True)
            print("Location search started...")
            gh.add_devices([pnh.device_considered_realName])
            gh.getLocations() # LOCATION SEARCH
            print("Location search completed.")
            nh = create_network(pnh, gh, log = False)
            post_processing(nh, gh, log = False)
        except DataError as e:
            print(e.args[0])
    elif mode == "all":
        print("RUN started...")
        gh = General_handler()
        pnhs = [] # LIST OF THE PRE-NETWORK HANDLERS
        i = 1
        while i <= len(config.true_device_names):
            for p in priority:
                print("File " + str(i) + " with priority " + p + ":")
                try:
                    pnh = preprocess_network(p, i, gh, log = False)
                    gh.add_devices([pnh.device_considered_realName])
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
                nh = create_network(pnh, gh, log = False)
                post_processing(nh, gh, log = False)
            except DataError as e:
                print(e.args[0])
            else:
                print("Network creation completed.")
        
        #gh.save_to_file()        
    print("RUN completed")
    
def clearDirectory(folder):
    # Delete all file in output:
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)
    
if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    # run the network creation:
    run_script(mode)

