'''
Created on 05 feb 2018
@author: Alessandro Pozzi
Config file. Can also run the entire system
'''


''' GENERAL settings '''
CORRELATION_MINUTES = 5
FILE_SUFFIX = "afterDup" #clusters_mc_dbscan, afterNoDup, afterStateNodup, clusters_offline_average1x, clusters_static_distance12sec
                            #clusters_offline_average1x, clusters_static_distance12sec, clusters_meanShift, clusters_averageDeviation...
                            #afterNoDupClustering_avg+stdev, beforeTS_5min, customNoDup
EXTRA = "" # state, tag, description, livelloPriorita "" : use this to select the extra info to attach to Device (needed also in main).
CORRELATION_UNIQUENESS = True # Used when computing the LIFT, in DatabaseNetworkCorrelator. If True, will consider only ONCE events
                                # happened multiple times after each "n" minutes block. In general, leave this True

''' VALIDATION settings'''
WINDOW = "after" #after, before | ALSO VALID FOR GRAPHS AND REF DEVICE IN MC
#---- RICORDATI di cambiare il FILE_SUFFIX in "timestampBefore" o "timestampAfter" + AGGIUNGI "tag" in EXTRA
VALIDATION_NAME = "validation"

''' Multiple reference devices '''
#Usa FILE_SUFFIX "CUSTOM" for the multiple reference device e genera la rete solo sul file di nome "custom" e la priorita' L0
#FIXED_NETWORK_DEVICES = ["AUTO-TRANSFERT", "EMD1A*9", "EMD2A*9", "EMD3A*9", "EMC700/1E"] #, "ECC01/5DX"
#FIXED_NETWORK_DEVICES = ["ESS11*13", "EBS1/28", "EBS1/22", "EBS132/2X"]
#FIXED_NETWORK_DEVICES = ["AUTO-TRANSFERT", "ECC01/5DX", "EMD102*43", "EMC001*9", "EMC700/1E" ]
#FIXED_NETWORK_DEVICES = ["EXS311*80", "EBS132/2X", "EBS1/28", "ESS11*13" ]                     
#FIXED_NETWORK_DEVICES = ["ESS11*13", "EBS132/2X", "EBS1/28", "EXS311*80" ]         
#FIXED_NETWORK_DEVICES = ["EAS11/8H", "ESS11/6A", "EAS11/2HB"]
FIXED_NETWORK_DEVICES = ["ECE001*9", "ECE001/BE", "EKD208/6E", "ECE001/8E", "EKD203/5E"]
                       
'''markov settings'''
clustering = "mean_shift" # no_clustering, mean_shift, db_scan, avg_plus_stdev, offline_average, static_distance
variance = True
timestamp = False #set to use CERN timestamp's
occurrencesAsBN = True #Metti True per contare le occorrenze come nelle BN
#chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS11/5H', 'ESS1*84',
#                'ESS406/E91', 'ESS520/E91'] #, 'ECD1*62'], 'ESS520/E91', 'ESS407/E91',
#chosenDevices = ['ERD15*45']

''' expandDevice settings '''
chosenDevices = ['EHS60/BE', 'EMC001*9', 'EXS106/2X', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91']
#chosenDevices = ['EXS106/2X']
#              'EMD101/8E','EMD102/8E','EMD103/8E','EMD407/8E','EMD202/8E','EMD301/8E','EMD206/8E','EKC200/8U']
#'ESS1*84'
#chosenDevices = ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X'] #our devices
levelsOfPriority = ['L0', 'L1', 'L2', 'L3']
unitePriorities = False #If this is true, all the priority above will be put together (automatically even in the main if mode="one")

''' main settings '''
file_selection = 4 # 1 to the len of the lists below (only for "one" mode. Select in the lists below)
selectPriority = 'L2' # 'L0', 'L1', 'L2', 'L3' -- ONLY FOR MODE=="ONE". If unitePriorities = True --> this will be forced to "L0"
#escaped_file_names = "EMC0019", "EHS60BE", "ESS115H", "ESS184", "EXS48X", "EXS1062X"]
#                    'ESS406E91', 'ESS407E91', 'ESS520E91', 'ESS1184']
#                   'ECD162']
#['EMD1018E','EMD1028E','EMD1038E','EMD4078E','EMD2028E','EMD3018E','EMD2068E','EKC2008U']
escaped_file_names = ['custom', 'EHS60BE', 'EXS48X', 'EMC0019', 'EXS1062X', 'ESS115H', 'ESS184',
                     'ESS406E91', 'ESS407E91', 'ESS520E91', 'ERD1545']#, 'ECD162']'custom',
escaped_file_names = ['EHS60BE', 'EMC0019', 'EXS1062X', 'ESS115H', 'ESS184', 'EXS48X', 'ESS406E91', 'ESS407E91', 'ESS520E91']
    #            'EMD1018E','EMD1028E','EMD1038E','EMD4078E','EMD2028E','EMD3018E','EMD2068E','EKC2008U']
#true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X',
#                    'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
#true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
#                    'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
#                    'ECD1*62']
true_device_names = ['custom','EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS11/5H', 'ESS1*84'
                     , 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ERD15*45']#, 'ECD1*62']#, 'ESS11*84', 'custom',
true_device_names = ['EHS60/BE', 'EMC001*9', 'EXS106/2X', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91']
    #            'EMD101/8E','EMD102/8E','EMD103/8E','EMD407/8E','EMD202/8E','EMD301/8E','EMD206/8E','EKC200/8U']
#true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
#true_device_names = ['EMD101/8E','EMD102/8E','EMD103/8E','EMD407/8E','EMD202/8E','EMD301/8E','EMD206/8E','EKC200/8U']
                    #'validation'
mode = "one" #one, all  | "one" to do the single file-priority selected above;
                        # "all" to do all the possible files and priorities in the lists above

#import main
#import expandDeviceTestGraphs, expandDeviceClustering


def runAll():
    #expandDeviceTestGraphs.searchItemsets()
    #expandDeviceClustering.searchItemsets()
    #main.run_script(mode)
    pass


#if __name__ == "__main__":
    #runAll()