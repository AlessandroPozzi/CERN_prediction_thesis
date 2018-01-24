import mysql.connector  # pip install mysql-connector-python
from pymining import itemmining # pip install pymining  
from File_writer import File_writer
from StateOfDevices import StateHandler

support = 0.5

#THIS IS THE expandDevice WITH THE STATES that REMOVE OVERLAPS and REMOVE DUPLICATES
def compareChosenDevicesByAlarmPriority(cursor):
    #chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84',
    #                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
    # our devices: ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    chosenDevices = ["AUTO-TRANSFERT", "ECC01/5DX", "EMD1A*9", "EMD2A*9", "EMD3A*9", "EMC700/1E", "EMC001*9"]
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']


    fw = File_writer("CUSTOM_7net-nooverlaps-yesallsingledup-emc001") #always leave "CUSTOM_" in front
    fw2 = File_writer("last_SQL_query_results")
    fw.create_txt("../res/newres/")
    fw2.create_txt("../res/newres/")
    print '\nDEVICE '+ str(chosenDevices) + ': '
    fw.write_txt('\nDEVICE '+ str(chosenDevices) + ': ')
    
    #print '\n\tPRIORITY ' + str(l) + ':'
    fw.write_txt('\n\tPRIORITY L0 :')
    strList = "%s"
    for i in range(1, len(chosenDevices)):
        strList = strList + " OR device=" + "%s"
    textQuery = "select * from electric where (device=" + strList + ") and action='Alarm CAME' order by time"
    print(textQuery)
    query = (textQuery)
    tpl = tuple(chosenDevices)
    cursor.execute(query, tpl)
    events = cursor.fetchall()
    afterSeq = [] # Contiene le liste dei device che vediamo in ogni riga nei file di testo
    beforeSeq = []
    afterSequence = [] # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
    beforeSequence = []
    intersectionDevicesBeforeAndAfter = []

    stateOfDevices = StateHandler()
    totalEvents = 0
    totalRelevantEvents = 0
    for e in events:
        #print '\n' + str(e)
        totalEvents = totalEvents + 1
        totalRelevantEvents += 1
        '''
        query = ("select * from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
        cursor.execute(query, (e[0], e[0], 5))
        eventsAfter = cursor.fetchall()
        '''
        devicesAfter = []  # all events that happened 5 min after the event "e"
        if not stateOfDevices.addActivatedDevice(e[4], e[0]): #i.e. sequence is ready
            sequence = stateOfDevices.getSequence()
            afterSeq.append(sequence)
            stateOfDevices = StateHandler() #create new StateHandler
            stateOfDevices.addActivatedDevice(e[4], e[0])
        elif stateOfDevices.check_device_ready(): #if we have a single device sequence ready..
            single_sequence = stateOfDevices.get_device_ready()
            afterSeq.append(single_sequence)
        
        '''
        #totalEvents = totalEvents + len(eventsAfter)
        for ea in eventsAfter:
            totalEvents = totalEvents + 1
            if ea[4] in chosenDevices:
                totalRelevantEvents += 1
                devicesAfter.append(ea[4]) #ea[4] = nome del device
                if not stateOfDevices.addActivatedDevice(ea[4], ea[0]):
                    sequence = stateOfDevices.getSequence()
                    afterSeq.append(sequence)
                    stateOfDevices = StateHandler() #create new StateHandler
                    stateOfDevices.addActivatedDevice(ea[4], ea[0])
                elif stateOfDevices.check_device_ready(): #if we have a single device sequence ready..
                    single_sequence = stateOfDevices.get_device_ready()
                    afterSeq.append(single_sequence)
        '''
        
        #if devicesAfter != []:
        #    devicesAfter.append(e[4])
        #afterSequence.append(devicesAfter) # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
        #devicesAfter=list(set(devicesAfter)) #Lista non ordinata di distinct devices
        #afterSeq.append(devicesAfter) #Lista di liste (i.e. tutti quello dopo "distinct device after 5 min")
        if e[4] in ["ECC01/5DX"]:
            print(e[4] + " - Timestamp " + str(e[0]))
            fw2.write_txt(e[4]  + " - Timestamp " + str(e[0]))

    #add the last sequence
    sequence = stateOfDevices.getSequence()
    afterSeq.append(sequence)
    
    '''
    # CONSOLE
    print '========= AFTER ========='
    print '\t\tSequences After: [ '
    for xx in afterSequence:
        print '\t\t[ ',
        for yy in xx:
            print "'" + str(yy) + "',",
        print '], '
    print ']'
        ghjk
    # TXT FILE
    fw.write_txt( '========= AFTER =========')
    fw.write_txt( '\t\tSequences After: [ ' )
    for xx in afterSequence:
        fw.write_inline( '\t\t[ ' ) 
        for yy in xx:
            fw.write_inline( "'" + str(yy) + "'," )
        fw.write_txt( '], ' )
    fw.write_txt( ']' ) 
    '''
        
    #CONSOLE
    print '\n\t\tDistinct devices after 5 minutes: [ '
    for xx in afterSeq:
        print '\t\t[ ',
        for yy in xx:
            print "'" + str(yy) + "', ",
        print '], '
    print ']'
    
    #TEXT FILE
    fw.write_txt('\n\t\tDistinct devices after 5 minutes: [ ')
    for xx in afterSeq:
        fw.write_inline( '\t\t[ ', )
        for yy in xx:
            fw.write_inline( "'" + str(yy) + "', " )
        fw.write_txt('], ')
    fw.write_txt(']')

    '''
    relim_input = itemmining.get_relim_input(afterSeq)
    report = itemmining.relim(relim_input, min_support=int(afterSeq.__len__() * support))
    print "\n\t\t  ===> FREQUENT ITEMSETS in Distinct devices after 5 minutes: (with support=0.5 the threshold is " + str(afterSeq.__len__() * support) + ")"
    for key, values in report.items():
        print '\t\t\t',
        print key,
        print ': ',
        print values
    '''
    '''
    #CONSOLE
    print '========= BEFORE ========='
    print '\t\tSequences Before: [ '
    for xx in beforeSequence:
        print '\t\t[ ',
        for yy in xx:
            print "'" + str(yy) + "', ",
        print '], '
    print ']'
    
    #TXT FILE
    fw.write_txt( '========= BEFORE =========' )
    fw.write_txt( '\t\tSequences Before: [ ' )
    for xx in beforeSequence:
        fw.write_inline( '\t\t[ ' )
        for yy in xx:
            fw.write_inline( "'" + str(yy) + "', " )
        fw.write_txt( '], ' )
    fw.write_txt( ']' )
    '''
    
    '''
    #CONSOLE
    print '\t\tDistinct devices before: [ '
    for xx in beforeSeq:
        print '\t\t[ ',
        for yy in xx:
            print "'" + str(yy) + "', ",
        print '], '
    print ']'
    
    #TXT FILE
    fw.write_txt( '\t\tDistinct devices before: [ ' )
    for xx in beforeSeq:
        fw.write_inline( '\t\t[ ' )
        for yy in xx:
            fw.write_inline( "'" + str(yy) + "', " )
        fw.write_txt( '], ' ) 
    fw.write_txt( ']' )
    '''

    print("EVENTI TOTALI NEL DOPPIO CICLO DI QUERY: " + str(totalEvents))
    print("EVENTI TOTALI UNICAMENTE RELATIVI AI DEVICE PRESCELTI NEL DOPPIO CICLO DI QUERY: " + str(totalRelevantEvents))
    counterEvents = 0
    counterItemsets = 0
    for lst in afterSeq:
        counterEvents = counterEvents + len(lst)
        counterItemsets = counterItemsets + 1
    print("NUMERO DI ITEMSET TOTALI GENERATI: " + str(counterItemsets))
    print("EVENTI TOTALI IN TUTTI GLI ITEMSET GENERATI: " + str(counterEvents))

    stateOfDevices.debugFile()

    
    #relim_input = itemmining.get_relim_input(beforeSeq)
    #report = itemmining.relim(relim_input, min_support=int(beforeSeq.__len__() * support))
    # print "\t\t  Frequent Itemsets in Distinct devices before: (soglia=" + str(beforeSeq.__len__()*support) + ")"
    # for key, values in report.items():
    #     print '\t\t\t',
    #     print key,
    #     print ': ',
    #     print values




cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
cursor = cnx.cursor()
compareChosenDevicesByAlarmPriority(cursor)


