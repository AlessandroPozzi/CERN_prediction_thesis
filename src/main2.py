'''
Created on 28 dic 2017
@author: Alessandro Corsair

This is a (probably only temporary) module that will use the Log_extractor class to deal with CERN's log raw data.
'''
import csv
from Log_extractor import Log_extractor
from File_writer import File_writer


log_extractor = Log_extractor()

#log_extractor.extract_raw_data() #RUN THIS ONLY IF YOU STILL DON'T HAVE THE UNIQUE 2016 .CSV
file_devices = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']

#d = log_extractor.count_occurrences(file_devices)
#print(d)

device_occurrences = log_extractor.findtop_occurrences()
with open('../output/alldevices_occurrences.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(["Device", "Occurrences"])
    for devocc in device_occurrences:
        writer.writerow([devocc[0], devocc[1]])
    
print(device_occurrences)

