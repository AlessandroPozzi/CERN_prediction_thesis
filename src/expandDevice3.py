import mysql.connector  # pip install mysql-connector-python
from pymining import itemmining # pip install pymining  
from File_writer import File_writer

support = 0.5

#THIS IS THE expandDevice WITH the OVERLAPPING and DUPLICATES
#PER LE RETI CON DISPOSITIVI ARBITRARI
def compareChosenDevicesByAlarmPriority(cursor):
    #chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84',
    #                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
    # our devices: ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    chosenDevices = ["AUTO-TRANSFERT", "ECC01/5DX", "EMD1A*9", "EMD2A*9", "EMD3A*9", "EMC700/1E"]
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']


    fw = File_writer("CUSTOM_7net-overlaps-emc001")
    fw.create_txt("../res/newres/")
    print '\nDEVICE '+ str(chosenDevices) + ': '
    fw.write_txt('\nDEVICE '+ str(chosenDevices) + ': ')
    
    #print '\n\tPRIORITY ' + str(l) + ':'
    fw.write_txt('\n\tPRIORITY L0 :')
    strList = "%s"
    for i in range(1, len(chosenDevices)):
        strList = strList + " OR device=" + "%s"
    textQuery = "select * from electric where device=" + strList + " and action='Alarm CAME'"
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

    for e in events:
        #print '\n' + str(e)
        query = ("select * from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
        cursor.execute(query, (e[0], e[0], 5))
        eventsAfter = cursor.fetchall()
        devicesAfter = []  # all events that happened 5 min after the event "e"
        for ea in eventsAfter:
            if ea[4] in chosenDevices:
                devicesAfter.append(ea[4]) #ea[4] = nome del device
        
        #if devicesAfter != []:
        devicesAfter.append(e[4])

        afterSequence.append(devicesAfter) # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
        devicesAfter=list(set(devicesAfter)) #Lista non ordinata di distinct devices
        afterSeq.append(devicesAfter) #Lista di liste (i.e. tutti quello dopo "distinct device after 5 min")
        print(e[4] + str(devicesAfter) + " - ID " + str(e[21]))
        
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

  


cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
cursor = cnx.cursor()
compareChosenDevicesByAlarmPriority(cursor)


