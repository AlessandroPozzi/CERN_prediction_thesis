'''
Created on 05 feb 2018
@author: Alessandro Pozzi
Config file. Can also run the entire system
'''


''' GENERAL settings '''
CORRELATION_MINUTES = 5
FILE_SUFFIX = "afterNoDup" #clusters_dbscan, afterNoDup, afterStateNodup, clusters_offline_average1x, clusters_static_distance12sec 
                            #clusters_offline_average1x, clusters_static_distance12sec, clusters_meanShift, clusters_averageDeviation...
CORRELATION_UNIQUENESS = True # Used when computing the LIFT, in DatabaseNetworkCorrelator. If True, will consider only ONCE events
                                # happened multiple times after each "n" minutes block. In general, leave this True

''' expandDevice settings '''
chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84', 'ESS11/5H']
#                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
 #chosenDevices = ['ECD1*62']
#chosenDevices = ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X'] #our devices
levelsOfPriority = ['L0', 'L1', 'L2', 'L3']
unitePriorities = False #If this is true, all the priority above will be put together (automatically even in the main if mode="one")

''' main settings '''
file_selection = 1  # 1 to the len of the lists below (only for "one" mode. Select in the lists below)
selectPriority = 'L1' # 'L0', 'L1', 'L2', 'L3' -- ONLY FOR MODE=="ONE". If unitePriorities = True --> this will be forced to "L0"
escaped_file_names = ["EMC0019", "EHS60BE", "ESS115H", "ESS184","EXS48X", "EXS1062X"]
#                    'ESS406E91', 'ESS407E91', 'ESS520E91', 'ESS1184', "CUSTOM"]
#escaped_file_names = ['ECD162'] #For the txt name
true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X'] 
#                    'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84', 'CUSTOM']
#true_device_names = ['ECD1*62']
mode = "all" #one, all  | "one" to do the single file-priority selected above; 
                        # "all" to do all the possible files and priorities in the lists above

import main
import expandDevice, expandDeviceClustering


def runAll():
    #expandDevice.searchItemsets()
    #expandDeviceClustering.searchItemsets()
    main.run_script("all")


if __name__ == "__main__":
    runAll()