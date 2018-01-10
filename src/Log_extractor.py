#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 28 dic 2017

@author: Alessandro Corsair
'''
import pandas as pd
from DataError import DataError

class Log_extractor(object):
    '''
    Handles the original (.csv) data of the CERN
    '''

    def __init__(self):
        '''
        '''
    
    def extract_raw_data(self):
        ''' Concatenates the .csv file of 2016 and creates a united .csv in ../res/2016/ '''
        df = pd.read_csv('../res/2016/2016_S1.csv')
        for i in range(2,52):
            df2 = pd.read_csv('../res/2016/2016_S{0}.csv'.format(str(i)))
            df = pd.concat([df, df2])
            df.to_csv('../res/2016_data.csv')
            print len(df)
            
    def find_location(self, devices):
        ''' 
        Finds the area of each of the devices in the list.
        Zone = H0 or H1 or H2
        Returns a dictionary with key = device and value = zone
        '''
        df = pd.read_csv('../res/2016_data.csv')
        d = dict() 
        for index, row in df.iterrows():
            H0 = df.loc[index, 'H0']
            H1 = df.loc[index, 'H1']
            H2 = df.loc[index, 'H2']
            H0 = unicode(H0, 'mbcs')
            H1 = unicode(H1, 'mbcs')
            H2 = unicode(H2, 'mbcs')
            device = df.loc[index, 'Device']
            if device in devices:
                d[device] = (H0, H1, H2)
                devices.remove(device)
            if not devices:
                return d
        return DataError("At least one of the devices is not in the .csv file")
    
    
    def count_occurrences(self, devices):
        ''' Counts the occurrences in the .csv of all devices in the given list '''
        df = pd.read_csv('../res/2016_data.csv')
        device_occurrence = dict()
        for index, row in df.iterrows():
            dev = df.loc[index, 'Device']
            if dev in devices:
                if dev not in device_occurrence: #key not present
                    device_occurrence[dev] = 1
                else:
                    device_occurrence[dev] = device_occurrence[dev] + 1
        return device_occurrence
    
    
    def findtop_occurrences(self):
        df = pd.read_csv('../res/2016_data.csv')
        device_occurrence = dict()
        for index, row in df.iterrows():
            dev = df.loc[index, 'Device']
            if dev not in device_occurrence: #key not present
                device_occurrence[dev] = 1
            else:
                device_occurrence[dev] = device_occurrence[dev] + 1
            if index % 1000 == 0:
                print(index)
                
        sorted_list = []
        for key in device_occurrence:
            sorted_list.append((key, device_occurrence[key]))
        sorted_list.sort(key = lambda tup: tup[1], reverse=True)
        
        return sorted_list
        