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