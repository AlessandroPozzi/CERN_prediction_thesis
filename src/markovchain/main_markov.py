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
import config

true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X',
                                  'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84', 'CUSTOM']
priority = ('L0', 'L1', 'L2', 'L3')  # Hard coded priority, do NOT change
select_priority = 'L1'  # 'L0', 'L1', 'L2', 'L3' -- ONLY FOR MODE=="ONE"
file_selection = 1  # 1 to 11 -->  ("EMC0019", "EHS60BE", "ES115H", "ESS184", "EXS48X", "EXS1062X",
#   'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84', "CUSTOM"]
# use "CUSTOM" (file number 11) in mode="one" to generate a custom network from expandDevice2 (SET PRIORITY L0)
mode = "one"  # one, all  | "one" to do the single file-priority selected above;
# "all" to do all the possible files and priorities

def preprocess_network(select_priority, file_selection, gh, log):
    pre_markov_handler = Pre_markov_handler(gh)

    # 1) PROCESS FILES
    file_suffix = "_" + config.FILE_SUFFIX
    pre_markov_handler.process_files(select_priority, file_selection, file_suffix, log)

    # 2) SELECT VARIABLES
    var_type = "frequency"  # occurrences, frequency, variance_only, support_variance
    support = 0.4
    MIN = 4
    MAX = 6
    pre_markov_handler.select_variables(var_type, MIN, MAX, support, log)

    # *) COLUMNS INFO (state, tag, description)
    pre_markov_handler.checkColumnDistribution()

    # RETURN the pre_network_handler object
    return pre_markov_handler

def create_markov_chain(pnh, gh):
    markov_handler = MarkovHandler(pnh, gh)

    # 4) LEARN THE MODEL
    markov_handler.create_mc_model()

    # 5) DRAW THE NETWORK
    location_choice = False  # True, False
    info_choice = True  # True, False
    avg_var_edges = True # True, False
    markov_handler.draw_mc_model(location_choice, info_choice, avg_var_edges)


''' The main script to create the BNs '''


def run_script(mode):
    if mode == "one":
        try:
            gh = General_handler()
            file_selected = true_device_names[file_selection - 1]  # true device name
            expandDeviceMarkov.create_sequences_txt(file_selected)
            pnh = preprocess_network(select_priority, file_selection, gh, log=True)
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
        while i <= 6:
            file_selected = true_device_names[i - 1]  # true device name
            expandDeviceMarkov.create_sequences_txt(file_selected)
            for p in priority:
                print("File " + str(i) + " with priority " + p + ":")
                try:
                    pnh = preprocess_network(p, i, gh, log=False)
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
    run_script(mode)

