'''
QUESTO FILE e' lo script originale expandDeviceTestGraphs.py ripulito dalle parti per noi inutili
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
# from helpers.File_writer import File_writer
from File_writer import File_writer
from ClusteringHandler import ClusterHandler
from DataError import DataError
import re
import config
import pomegranate as pomgr
import columnAnalyzer
from datetime import timedelta
import string

def create_sequences_txt(device):
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    if config.clustering != "no_clustering":
        sequences = __create_txt_clusters(cursor, device)
    else:
        sequences = __create_txt_noClusters(cursor, device)
    cursor.close()
    return sequences

def create_mc_model(device, priority, consideredDevices, sequences):
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    (mc, avg_var_list, ref_dev_avg_vars) = __get_markov_chain_model(cursor, device, priority, consideredDevices, sequences)
    cursor.close()
    return (mc, avg_var_list, ref_dev_avg_vars)


def __create_txt_noClusters(cursor, d):

    print '\n\nYou are not using clustering\n'
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']
    fw = File_writer(d, config.FILE_SUFFIX)
    fw.create_txt("../../res/")
    markedEvents = []
    print '\nDEVICE ' + str(d) + ': '
    fw.write_txt('\nDEVICE ' + str(d) + ': ')
    afterSeqPriority = [] # Contiene tuple di liste con priorita associata
    for l in levelsOfPriority:
        print '\n\tPRIORITY ' + str(l) + ':'
        fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
        query = ("select * from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
        cursor.execute(query, (d, l))
        events = cursor.fetchall()
        afterSeq = []  # Contiene le liste dei device che vediamo in ogni riga nei file di testo

        for e in events:
            if config.WINDOW == "after":
                # Query on the AFTER
                query = (
                "select * from electric where action='Alarm CAME' and Time >= (%s) and Time <= (%s + interval %s minute) order by time")
            elif config.WINDOW == "before":
                # Query on the BEFORE
                query = (
                "select * from electric where action='Alarm CAME' and Time <= (%s) and Time >= (%s - interval %s minute) order by time")
            cursor.execute(query, (e[0], e[0], config.CORRELATION_MINUTES))
            eventsAfter = cursor.fetchall()
            devicesAfter = []  # all events that happened 5 min after the event "e"
            for ea in eventsAfter:
                if ea not in markedEvents:  # CONDIZIONE per rimuovere i DUPLICATI
                    markedEvents.append(ea)
                    if ea[4] != d:  # CONDIZIONE per evitare problemi con il device di riferimento e l'aggiunta di stati, tag o descr.
                        if config.EXTRA == "state":
                            index = 10
                        elif config.EXTRA == "tag":
                            index = 5
                        elif config.EXTRA == "description":
                            index = 6
                        elif config.EXTRA == "livelloPriorita":
                            index = 22
                        if config.EXTRA:
                            extraColumn = ea[index].encode('ascii', 'ignore').decode('ascii')
                            extraColumn = extraColumn.replace("'", "")
                            #extraColumn = re.escape(extraColumn)
                        else:
                            extraColumn = ""
                        devicesAfter.append(ea[4] + "--" + extraColumn)
            if devicesAfter != []:
                afterSeq.append(devicesAfter)  # Lista di liste (i.e. tutto quello dopo "distinct device after 5 min")
                afterSeqPriority.append((devicesAfter, l))

        # CONSOLE
        print '\n\t\tSequences of activated devices after 5 minutes: [ '
        for xx in afterSeq:
            print '\t\t[ ',
            for yy in xx:
                print "'" + str(yy) + "', ",
            print '], '
        print ']'

        # TEXT FILE
        fw.write_txt('\n\t\tSequences of activated devices after 5 minutes: [ ')
        for xx in afterSeq:
            fw.write_inline('\t\t[ ', )
            for yy in xx:
                fw.write_inline("'" + str(yy) + "', ")
            fw.write_txt('], ')
        fw.write_txt(']')

        print("==>")
        fw.write_txt('==>', newline=True)  # KEEP THIS!

    return afterSeqPriority


def __create_txt_clusters(cursor, d):
    print 'You have chosen to use clustering'
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']
    fw = File_writer(d, config.FILE_SUFFIX)
    fw.create_txt("../../res/")
    fw2 = File_writer(d, "DEBUG-" + config.FILE_SUFFIX)
    fw2.create_txt("../../res/debug/")
    print '\nDEVICE ' + str(d) + ': '
    fw.write_txt('\nDEVICE ' + str(d) + ': ')
    clusterListPriority = [] #list of tuples with sequences and priority associated
    for l in levelsOfPriority:
        print '\n\tPRIORITY ' + str(l) + ':'
        fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
        query = (
        "select * from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
        cursor.execute(query, (d, l))
        events = cursor.fetchall()
        clusterList = []

        for e in events:
            if config.WINDOW == "after":
                # Query on the AFTER
                query = (
                "select * from electric where action='Alarm CAME' and Time >= (%s) and Time <= (%s + interval %s minute) order by time")
                cursor.execute(query, (e[0], e[0], config.CORRELATION_MINUTES))
            elif config.WINDOW == "before":
                # Query on the BEFORE
                query = (
                "select * from electric where action='Alarm CAME' and Time <= (%s) and Time >= (%s - interval %s minute) order by time")
                cursor.execute(query, (e[0], e[0], config.CORRELATION_MINUTES))

            clusterHandler = ClusterHandler(e)  # Initialize the ClusterHandler with the reference event
            eventsAfter = cursor.fetchall()
            for ea in eventsAfter:
                if ea != e:  # Ensures the reference event is not considered as a clustering point
                    clusterHandler.addEvent(ea)

            try:
                if config.clustering == "offline_average":
                    clusterHandler.findClustersOfflineAverage(fw2, debug=True)
                elif config.clustering == "static_distance":
                    timeDelta = timedelta(seconds = 5)
                    clusterHandler.findClustersStaticDistance(fw2, timeDelta, debug=True)
                elif config.clustering == "db_scan":
                    clusterHandler.findClustersDBSCAN(fw2, debug=True)
                elif config.clustering == "mean_shift":
                    clusterHandler.findClustersMeanShift(fw2, debug=True)
                elif config.clustering == "avg_plus_stdev":
                    clusterHandler.findClustersAverageDeviation(fw2, debug = True)
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
            elif config.EXTRA == "livelloPriorita":
                index = 22
            
            #Create the list of itemsets
            for stateList in newClusters:
                devices = []
                for evstate in stateList:
                    rawEvent = evstate.rawEvent
                    if config.EXTRA:
                        extraColumn = rawEvent[index].encode('ascii', 'ignore').decode('ascii')
                        extraColumn = extraColumn.replace("'", "")
                    else:
                        extraColumn = ""
                    devices.append(evstate.getDevice() + "--" + extraColumn)
                clusterList.append(devices)
                clusterListPriority.append((devices, l))

        # clusterList = a list of clusters, where each cluster is a list of devices
        # stateList = a list of objects "EventState"
        # evstate = a single object "EventState"

        # uncomment this if you want to see the itemsets printed to the console
        # CONSOLE
        print '\n\t\tSequences of activated devices after 5 minutes: [ '
        for devList in clusterList:
            print '\t\t[ ',
            for dev in devList:
                print "'" + dev + "', ",
            print '], '
        print ']'

        # TEXT FILE
        fw.write_txt('\n\t\tSequences of activated devices after 5 minutes: [ ')
        for devList in clusterList:
            fw.write_inline('\t\t[ ', )
            for dev in devList:
                fw.write_inline("'" + dev + "', ")
            fw.write_txt('], ')
        fw.write_txt(']')

        print("==>")
        fw.write_txt('==>', newline=True)

    return clusterListPriority


def __get_markov_chain_model(cursor, d, l, consideredDevices, sequences):

    avg_var_list = []
    ref_dev_avg_vars = []
    if config.variance == True:
        #get average and standard deviation for every couple of considered devices
        for sourceDev in consideredDevices:
            try:
                sourceOnlyDevName, sourceOnlyExtraName = string.split(sourceDev, "--")
                var_one_vs_all_full = columnAnalyzer.find_column_distribution(sourceOnlyDevName, ['L0','L1','L2','L3'], consideredDevices)
                for destDev in var_one_vs_all_full:
                    avg_var_list.append((sourceDev, destDev, var_one_vs_all_full[destDev].msAverage, var_one_vs_all_full[destDev].msStandDev))
            except:
                print 'Variable with name equal to ' + sourceDev + ' could not be processed'
        # get average and standard deviation for <reference device - other devices>
        var_one_vs_all_full = columnAnalyzer.find_column_distribution(d, ['L0', 'L1', 'L2', 'L3'], consideredDevices)
        for destDev in var_one_vs_all_full:
            ref_dev_avg_vars.append((d, destDev, var_one_vs_all_full[destDev].msAverage, var_one_vs_all_full[destDev].msStandDev))

    if sequences:
        mc = pomgr.MarkovChain.from_samples(sequences)
    else:
        raise DataError("No data in this device - priority combination")
    return (mc, avg_var_list, ref_dev_avg_vars)