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
#chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84', 'ESS11/5H']
#                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
#chosenDevices = ['ECD1*62','EMD101/8E','EMD102/8E','EMD103/8E','EMD407/8E','EMD202/8E','EMD301/8E','EMD206/8E','EKC200/8U']
chosenDevices = ['ECD1*62']
#chosenDevices = ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X'] #our devices
levelsOfPriority = ['L0', 'L1', 'L2', 'L3']
unitePriorities = False #If this is true, all the priority above will be put together (automatically even in the main if mode="one")

''' main settings '''
file_selection = 1  # 1 to the len of the lists below (only for "one" mode. Select in the lists below)
selectPriority = 'L0' # 'L0', 'L1', 'L2', 'L3' -- ONLY FOR MODE=="ONE". If unitePriorities = True --> this will be forced to "L0"
escaped_file_names = ["EMC0019", "EHS60BE", "ESS115H", "ESS184","EXS48X", "EXS1062X"]
#                    'ESS406E91', 'ESS407E91', 'ESS520E91', 'ESS1184', "CUSTOM"]
#escaped_file_names = ['EMD1018E','EMD1028E','EMD1038E','EMD4078E','EMD2028E','EMD3018E','EMD2068E','EKC2008U']
true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X'] 
#                    'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84', 'CUSTOM']
#true_device_names = ['EMD101/8E','EMD102/8E','EMD103/8E','EMD407/8E','EMD202/8E','EMD301/8E','EMD206/8E','EKC200/8U']
mode = "all" #one, all  | "one" to do the single file-priority selected above; 
                        # "all" to do all the possible files and priorities in the lists above

import main
import expandDevice, expandDeviceClustering


def runAll():
    #expandDevice.searchItemsets()
    #expandDeviceClustering.searchItemsets()
    main.run_script(mode)


if __name__ == "__main__":
    runAll()