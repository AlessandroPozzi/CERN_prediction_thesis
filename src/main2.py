'''
Created on 28 dic 2017
@author: Alessandro Corsair

This is a (probably only temporary) module that will use the Log_extractor class to deal with CERN's log raw data.
'''
from Log_extractor import Log_extractor

log_extractor = Log_extractor()
log_extractor.extract_raw_data() #RUN THIS ONLY IF YOU DON'T HAVE UNIQUE 2016 .CSV
