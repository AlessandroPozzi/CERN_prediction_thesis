# coding: utf-8
'''
Note: The following are my database columns (+ index). The order of columns you have must be the same.
Time, H0, H1, H2, Device, Tag, Description, PrevValue, Value, Unit, State, Action, Type, AlarmPriority, Week, Device_FirstLetter, 
   0,  1,  2,  3,      4,    5,          6,         7,      8,   9,    10,     11,   12,            13,   14,                 15,           

Device_SecondLetter_LevelOfTension, Device_ThirdLetter_Type, Device_Forth_Number, Device_Fifth_LHCspsMeyrin, Device_Sixth_Position, 
                                16,                      17,                  18,                        19,                    20, 

id, LivelloPriorita
21,              22
'''

import mysql.connector
from File_writer import File_writer
import config
from datetime import datetime
import json

'''
Extracts the critical timestamps from a file that contains data in json format
Returns a list that contains two lists: the first one is a list of the 48 timestamps that "fanno
saltare il fascio" and the second contains 3000+ (?) timestamps that "non fanno saltare il fascio"
'''
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



def compareChosenDevicesByAlarmPriority(cursor):

    [stop, nonStop] = loadGlitchesFromFile("glitchesDB.txt")
    stopList = [] #list 1 with timestamp (stop)
    for unixTS in stop:
        dtObject = datetime.fromtimestamp(unixTS)
        stopList.append(dtObject)
    fw = File_writer(config.VALIDATION_NAME, config.FILE_SUFFIX, config.EXTRA)
    fw.create_txt("../../res/")
    # For legacy reasons we need to use a priority, even if there's actually no priority involved
    print '\n\tPRIORITY ' + ':' + "L0"
    fw.write_txt('\n\tPRIORITY ' + ':' + "L0")

    for ts in stopList:
        if config.WINDOW == "after":
            # Query on the AFTER
            query = ("select * from electric where action='Alarm CAME' and Time >= %s and Time <= (%s + interval %s minute) order by time")
            cursor.execute(query, (ts,ts,config.CORRELATION_MINUTES))
            events = cursor.fetchall()
        elif config.WINDOW == "before":
            # Query on the BEFORE
            query = ("select * from electric where action='Alarm CAME' and Time <= %s and Time >= (%s - interval %s minute) order by time")
            cursor.execute(query, (ts,ts,config.CORRELATION_MINUTES))
            events = cursor.fetchall()
        
        eventTagList = []
        for e in events:
            eventTagList.append(e[4] + "--" + e[5])
        eventTagListNodup = list(set(eventTagList))
        
      #  if eventTagListNodup != []:
        
        #CONSOLE
        print '\t\t[ ',
        for ee in eventTagListNodup:
            print "'" + ee + "', ",
        print ']'
        
        #TEXT FILE
        fw.write_inline( '\t\t[ ', )
        for ee in eventTagListNodup:
            fw.write_inline( "'" + ee + "', " )
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