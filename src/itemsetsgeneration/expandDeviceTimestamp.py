# coding: utf-8
'''
QUESTO FILE e' lo script originale expandDevice.py ripulito dalle parti per noi inutili 
e con l'aggiunta di una condizione sulla lista "markedEvents" per far si' che uno stesso evento
NON venga considerato piu' volte per via dei problemi della DOPPIA QUERY.
Inoltre, viene anche usato per aggiungere lo stato, la descrizione o il tag ai device nell'itemset.

Note: The following are my database columns (+ index). The order of columns you have must be the same.
Time, H0, H1, H2, Device, Tag, Description, PrevValue, Value, Unit, State, Action, Type, AlarmPriority, Week, Device_FirstLetter, 
   0,  1,  2,  3,      4,    5,          6,         7,      8,   9,    10,     11,   12,            13,   14,                 15,           

Device_SecondLetter_LevelOfTension, Device_ThirdLetter_Type, Device_Forth_Number, Device_Fifth_LHCspsMeyrin, Device_Sixth_Position, 
                                16,                      17,                  18,                        19,                    20, 

id, LivelloPriorita
21,              22
'''
import mysql.connector  # pip install mysql-connector-python
from File_writer import File_writer
import re
import config
from datetime import datetime
import os
import json
from blaze.expr.datetime import DateTime

def loadGlitchesFromFile(filename):
    glitches = json.load(open(filename))
    glitches  = {float(k):v for k,v in glitches.items()}
    stopBeam = []
    notStopBeam =[]
    stop_temp=dict()
    nostop_temp=dict()
    for item in glitches:
        if glitches[item][1] == True:
            stop_temp[item] = glitches[item]
        else:
            nostop_temp[item] = glitches[item]

    for k in stop_temp.items():
        stopBeam.append(k[0])
 
    for n in nostop_temp.items():
       notStopBeam.append(n[0])
    return [stopBeam, notStopBeam]
    # In[8]:
    
    
    
    #print(str(len(stop)))
    #print(stop)
    #return returnList


def compareChosenDevicesByAlarmPriority(cursor):

    #tsList = loadGlitchesFromFile("glitchesDB.txt")
    
    [stop, nonStop] = loadGlitchesFromFile("glitchesDB.txt")
    
    # In[9]:
    # In[7]:
    returnList = []
    for unixTS in stop:
        dtObject = datetime.fromtimestamp(unixTS)
        returnList.append(dtObject)
    
    chosenDevices = config.chosenDevices
    levelsOfPriority = config.levelsOfPriority

    for ts in returnList:
        fw = File_writer("new test", config.FILE_SUFFIX, config.EXTRA)
        fw.create_txt("../../res/")
        print '\nTIMESTAMP '+ str(ts) + ': '
        fw.write_txt('\nTIMESTAMP '+ str(ts) + ': ')
        print '\n\tPRIORITY ' + ':'
        fw.write_txt('\n\tPRIORITY ' + ':')
        query = ("select * from electric where action='Alarm CAME' and Time <= %s and Time >= (%s - %s minute) order by time")
        cursor.execute(query, (ts,ts,5))
        events = cursor.fetchall()

        #CONSOLE
        print '\n\t\tDistinct devices after 5 minutes: [ '
        print '\t\t[ ',
        for xx in events:
            print "'" + str(xx[4] + "--" + xx[5]) + "', ",
        print ']'
        
        #TEXT FILE
        fw.write_txt('\n\t\tDistinct devices after 5 minutes: [ ')
        fw.write_inline( '\t\t[ ', )
        for xx in events:
            fw.write_inline( "'" + xx[4] + "--" + xx[5] + "', " )
            fw.write_txt('], ')
        fw.write_txt(']')
    
        print("==>")
        fw.write_txt('==>', newline = True) #KEEP THIS!

def searchItemsets():
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    compareChosenDevicesByAlarmPriority(cursor)
    cursor.close()

if __name__ == "__main__":
    searchItemsets()