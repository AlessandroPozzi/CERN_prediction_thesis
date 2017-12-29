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
        df = pd.read_csv('../res/2016/2016_S1.csv')
        for i in range(2,52):
            df2 = pd.read_csv('../res/2016/2016_S{0}.csv'.format(str(i)))
            df = pd.concat([df, df2])
            df.to_csv('../res/2016_data.csv')
            print len(df)
            
    def find_location(self, devices, zone):
        ''' 
        Finds the area of each of the devices in the list.
        Zone = H0 or H1 or H2
        Returns a dictionary with key = device and value = zone
        '''
        df = pd.read_csv('../res/2016_data.csv')
        d = dict() 
        for index, row in df.iterrows():
            HX = df.loc[index, zone]
            device = df.loc[index, 'Device']
            if device in devices:
                d[device] = HX
                devices.remove(device)
            if not devices:
                return d
        return DataError("At least one of the devices is not in the .csv file")
        