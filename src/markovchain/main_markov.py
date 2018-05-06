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
from Pre_markov_handler import Pre_markov_handler
from MarkovHandler import MarkovHandler
import expandDeviceMarkov
import expandDeviceMarkovTimestamp
import config


priority = ('L0', 'L1', 'L2', 'L3')  # Hard coded priority, do NOT change

def preprocess_network(select_priority, file_selection, gh, sequences, log):
    pre_markov_handler = Pre_markov_handler(gh, sequences)

    # 1) PROCESS FILES
    if config.EXTRA:
        extra = "_" + config.EXTRA
    else:
        extra = ""
    file_suffix = "_" + config.FILE_SUFFIX #+ extra + "_MC"
    pre_markov_handler.process_files(select_priority, file_selection, file_suffix, log)

    # 2) SELECT VARIABLES
    var_type = "occurrences"  # occurrences, frequency, variance_only, support_variance, lift, couple_occurrences, manual
    support = 0.1 #it considers only variables with a support higher than the value chosen (with frequency criterion)
    MIN = 4 #minimum number of variables to consider in the network
    MAX = 4 #maximum number of variables to consider in the network
    manualList = [] # nomi delle variabili da aggiungere, senza doppio trattino (NO COPPIE)
    manualList = ['EBS132/2X--', 'EBS1/22--', 'EBS1/28--',  'EXS311*80--']#'ESS11*13--' 'ESS10/1DX--'
    pre_markov_handler.select_variables(var_type, MIN, MAX, support, log, manualList)

    # *) COLUMNS INFO (state, tag, description)
    pre_markov_handler.checkColumnDistribution()

    # RETURN the pre_network_handler object
    return pre_markov_handler

def create_markov_chain(pnh, gh):
    markov_handler = MarkovHandler(pnh, gh)

    # 4) LEARN THE MODEL
    markov_handler.create_mc_model(pnh.sequences)

    # 5) DRAW THE NETWORK
    location_choice = False # True, False | True means that location of devices (H0-H1) will be shown in the network
    info_choice = True # True, False | True means that the graph will show additional device statistics (avg, st.dev, occ..).
    # info_choice and location_choice can't be both True at the same time.
    avg_var_edges = False # True, False | True adds on every edge the avg and st.dev between two nodes.
    refDevice = False # True, False | True means that the reference device with blue arcs will be added to the network
    hideNames = False # True, False | True means that the device names will be crypted in the graph. Set it to false since CERN allowed to use original device names
    onlyH0 = False # True, False | If location_choice is true and onlyH0 is true, H1 won't be shown in the graph, but only H0
    markov_handler.draw_mc_model(location_choice, info_choice, avg_var_edges, refDevice, hideNames, onlyH0)


''' The main script to create the BNs '''


def run_script(mode):
    if mode == "one":
        try:
            gh = General_handler()
            file_selected = config.true_device_names[config.file_selection - 1]  # true device name
            if not config.timestamp:
                sequences = expandDeviceMarkov.create_sequences_txt(file_selected)
            else:
                sequences = expandDeviceMarkovTimestamp.create_sequences_txt()
            pnh = preprocess_network(config.selectPriority, config.file_selection, gh, sequences, log=True)
            print("Location search started...")
            gh.getLocations()  # LOCATION SEARCH
            print("Location search completed.")
            create_markov_chain(pnh, gh)
        except DataError as e:
            print(e.args[0])
    elif mode == "all":
        print("RUN started...")
        gh = General_handler()
        pnhs = []  # LIST OF THE PRE-MARKOV HANDLERS
        i = 1
        while i < len(config.true_device_names):
            file_selected = config.true_device_names[i - 1]  # true device name
            sequences = expandDeviceMarkov.create_sequences_txt(file_selected)
            for p in priority:
                print("File " + str(i) + " with priority " + p + ":")
                try:
                    pnh = preprocess_network(p, i, gh, sequences, log=False)
                    pnhs.append(pnh)
                except DataError as e:
                    print(e.args[0])
                else:
                    print("Preprocessing completed.")
            i = i + 1
        print("Location search started...")
        gh.getLocations()  # LOCATION SEARCH
        print("Location search completed.")

        for pnh in pnhs:
            # print("Network creation for file " + pnh.get_device() +
            #      " and priority " + pnh.get_priority() + " started...")
            try:
                print("Markov Chain creation for file " + pnh.get_device() +
                    " and priority " + pnh.get_priority() + " started...")
                create_markov_chain(pnh, gh)
            except DataError as e:
                print(e.args[0])
            else:
                print("Network creation completed.")

                # gh.save_to_file()
    print("RUN completed")


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    # delete all file in output:
    import os, shutil

    folder = '../../output/'
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        # elif os.path.isdir(file_path): shutil.rmtree(file_path) #per pulire anche le sottocartelle
        except Exception as e:
            print(e)
    # run the network creation:
    run_script(config.mode)

