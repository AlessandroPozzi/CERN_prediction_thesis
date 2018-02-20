'''
Created on 05 feb 2018
@author: Alessandro Pozzi
Config file
'''

CORRELATION_MINUTES = 5
FILE_SUFFIX = "afterNoDup" #clusters_dbscan, afterNoDup, afterStateNodup, clusters_offline_average1x, clusters_static_distance12sec 
                            #clusters_offline_average1x, clusters_static_distance12sec, clusters_meanShift, clusters_averageDeviation...
CORRELATION_UNIQUENESS = True # Used when computing the LIFT, in DatabaseNetworkCorrelator. If True, will consider only ONCE events
                                # happened multiple times after each "n" minutes block
escaped_file_names = ["EMC0019", "EHS60BE", "ESS115H", "ESS184","EXS48X", "EXS1062X",
                    'ESS406E91', 'ESS407E91', 'ESS520E91', 'ESS1184', "CUSTOM",
                    'ECD162']
true_device_names = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X', 
                    'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84', 'CUSTOM',
                    'ECD1*62']