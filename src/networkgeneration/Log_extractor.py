#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 28 dic 2017

@author: Alessandro Corsair
'''
import pandas as pd
import mysql.connector 
#from helpers.DataError import DataError
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
        devices = List of devices
        Returns a dictionary with key = device and value = (H0, H1)
        '''
        cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
        cursor = cnx.cursor()
        d = dict() 
        for dev in devices:
            query = "select Device, H0, H1, H2 from electric where device=%s LIMIT 1"
            tpl = (dev,)
            cursor.execute(query, tpl)
            events = cursor.fetchall()
            if not events:
                return DataError("At least one of the devices is not in the .csv file")
            e = events[0]
            H0 = e[1]
            H1 = e[2]
            #H0 = unicode(H0, 'mbcs')
            #H1 = unicode(H1, 'mbcs')
            d[dev] = (H0, H1)
        return d
    
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
        