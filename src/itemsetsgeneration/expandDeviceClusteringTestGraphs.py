'''
Note: The following are my database columns (+ index). The order of columns you have must be the same.
Time, H0, H1, H2, Device, Tag, Description, PrevValue, Value, Unit, State, Action, Type, AlarmPriority, Week, Device_FirstLetter, 
   0,  1,  2,  3,      4,    5,          6,         7,      8,   9,    10,     11,   12,            13,   14,                 15,           

Device_SecondLetter_LevelOfTension, Device_ThirdLetter_Type, Device_Forth_Number, Device_Fifth_LHCspsMeyrin, Device_Sixth_Position, id, LivelloPriorita
                                16,                      17,                  18,                        19,                    20, 21,              22
'''
import mysql.connector  # pip install mysql-connector-python
from File_writer import File_writer
from ClusteringHandler import ClusterHandler
from DataError import DataError
from datetime import timedelta
import config
from Graph_drawer import Graph_drawer

def createRange(x, step, end):
    finalList = []
    while x <= end:
        finalList.append(x)
        x += step
    return finalList

def compareChosenDevicesByAlarmPriority(cursor):
    #chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84',
    #                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
    # our devices: ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    chosenDevices = config.chosenDevices
    levelsOfPriority = config.levelsOfPriority
    GD = Graph_drawer(autoMax = 10, start = 0, step = 0.5)
    ally = []
    
    for d in chosenDevices:
        yRanges = createRange(0.00,0.5,10)
        for t in yRanges:
            minutes = t
            print("Minutes = " + str(minutes))
            y = []
            if config.EXTRA:
                extra = "_" + config.EXTRA
            else:
                extra = ""
            file_suffix = config.FILE_SUFFIX + extra
            fw = File_writer(d, file_suffix)
            fw.create_txt("../../res/")
            fw2 = File_writer(d, "DEBUG-" + file_suffix)
            fw2.create_txt("../../res/debug/")
            print '\nDEVICE '+ str(d) + ': '
            fw.write_txt('\nDEVICE '+ str(d) + ': ')
            for l in levelsOfPriority:
                duplicates = 0
                print '\n\tPRIORITY ' + str(l) + ':'
                fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
                query = ("select * from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
                cursor.execute(query, (d,l))
                events = cursor.fetchall()
                clusterList = []
    
                for e in events:
                    query = ("select * from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
                    cursor.execute(query, (e[0], e[0], minutes))
                    clusterHandler = ClusterHandler(e) #Initialize the ClusterHandler with the reference event
                    eventsAfter = cursor.fetchall()
                    
                    for ea in eventsAfter:
                        if ea != e: #Ensures the reference event is not considered as a clustering point
                            dup = clusterHandler.addEvent(ea)
                            if dup:
                                duplicates += dup
                    
                    try:
                        if config.clustering == "offline_average":
                            clusterHandler.findClustersOfflineAverage(fw2, debug=True)
                        elif config.clustering == "static_distance":
                            timeDelta = timedelta(seconds=5)
                            clusterHandler.findClustersStaticDistance(fw2, timeDelta, debug=True)
                        elif config.clustering == "db_scan":
                            clusterHandler.findClustersDBSCAN(fw2, debug=True)
                        elif config.clustering == "mean_shift":
                            clusterHandler.findClustersMeanShift(fw2, debug=True)
                        elif config.clustering == "avg_plus_stdev":
                            clusterHandler.findClustersAverageDeviation(fw2, debug=True)
                        else:
                            print "\nNON EXISTENT CLUSTERING METHOD\n"
    
                    except DataError as e:
                        pass
                    newClusters = clusterHandler.getClusters()
                    
                    #Check in the config if extras must be used
                    if config.EXTRA == "state":
                        index = 10
                    elif config.EXTRA == "tag":
                        index = 5
                    elif config.EXTRA == "description":
                        index = 6
                    
                        
                    #Create the list of itemsets
                    for stateList in newClusters:
                        devices = []
                        for evstate in stateList:
                            rawEvent = evstate.rawEvent
                            if config.EXTRA:
                                extraColumn = rawEvent[index].encode('ascii', 'ignore').decode('ascii')
                                extraColumn.replace("'", "")
                            else:
                                extraColumn = ""
                            devices.append(evstate.getDevice() + "--" + extraColumn)
                        clusterList.append(list(set(devices)))
                        
                #NUMBER OF DUPLICATES
                print("Number of duplicates: " + str(duplicates))
                if l =='L1':
                    y.append(duplicates)
                
        #ally.append(y)
        GD.addTrace(y, d)
        
    GD.draw()
            
    
                
def searchItemsets():
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    compareChosenDevicesByAlarmPriority(cursor)

if __name__ == "__main__":
    searchItemsets()
