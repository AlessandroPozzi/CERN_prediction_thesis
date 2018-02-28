import mysql.connector  # pip install mysql-connector-python
'''
from helpers.File_writer import File_writer
from clustering.ClusteringHandler import ClusterHandler
from helpers.DataError import DataError
'''
from File_writer import File_writer
from ClusteringHandler import ClusterHandler
from DataError import DataError
import config

def compareChosenDevicesByAlarmPriority(cursor):
    #chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84',
    #                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
    # our devices: ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    chosenDevices = ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']

    for d in chosenDevices:
        fw = File_writer(d, config.FILE_SUFFIX)
        fw.create_txt("../../res/")
        fw2 = File_writer(d, "DEBUG-" + config.FILE_SUFFIX)
        fw2.create_txt("../../res/debug/")
        print '\nDEVICE '+ str(d) + ': '
        fw.write_txt('\nDEVICE '+ str(d) + ': ')
        for l in levelsOfPriority:
            print '\n\tPRIORITY ' + str(l) + ':'
            fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
            query = ("select * from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
            cursor.execute(query, (d,l))
            events = cursor.fetchall()
            clusterList = []

            for e in events:
                query = ("select * from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
                cursor.execute(query, (e[0], e[0], config.CORRELATION_MINUTES))
                clusterHandler = ClusterHandler(e) #Initialize the ClusterHandler with the reference event
                eventsAfter = cursor.fetchall()
                
                for ea in eventsAfter:
                    if ea != e: #Ensures the reference event is not considered as a clustering point
                        clusterHandler.addEvent(ea)
                
                try:
                    # 1) OFFLINE AVERAGE
                    #clusterHandler.findClustersOfflineAverage(fw2, debug=True)
                    # 2) STATIC DISTANCE
                    #timeDelta = timedelta(seconds = 5)
                    #clusterHandler.findClustersStaticDistance(fw2, timeDelta, debug=True)
                    # 3) DBSCAN
                    #clusterHandler.findClustersDBSCAN(fw2, debug=True)
                    # 4) MEAN SHIFT
                    clusterHandler.findClustersMeanShift(fw2, debug=True)
                    # 5) AVERAGE + STANDARD DEVIATION
                    #clusterHandler.findClustersAverageDeviation(fw2, debug = True)
                    
                except DataError as e:
                    pass
                newClusters = clusterHandler.getClusters()
                
                #Create the list of itemsets
                for stateList in newClusters:
                    devices = []
                    for evstate in stateList:
                        devices.append(evstate.getDevice())
                    clusterList.append(list(set(devices)))

            #clusterList = a list of clusters, where each cluster is a list of devices
            #stateList = a list of objects "EventState"
            #evstate = a single object "EventState"
            
            #uncomment this if you want to see the itemsets printed to the console
            #CONSOLE 
            print '\n\t\tDistinct devices after 5 minutes: [ '
            for devList in clusterList: 
                print '\t\t[ ',
                for dev in devList:
                    print "'" + dev + "', ",
                print '], '
            print ']'
            
                    
            #TEXT FILE
            fw.write_txt('\n\t\tDistinct devices after 5 minutes: [ ')
            for devList in clusterList:
                fw.write_inline( '\t\t[ ', )
                for dev in devList:
                    fw.write_inline( "'" + dev + "', " )
                fw.write_txt('], ')
            fw.write_txt(']')
            
            print("==>")
            fw.write_txt('==>', newline = True)
            
def searchItemsets():
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    compareChosenDevicesByAlarmPriority(cursor)

if __name__ == "__main__":
    searchItemsets()
