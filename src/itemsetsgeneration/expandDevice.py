'''
QUESTO FILE e' lo script originale expandDevice.py ripulito dalle parti per noi inutili 
e con l'aggiunta di una condizione sulla lista "markedEvents" per far si' che uno stesso evento
NON venga considerato piu' volte per via dei problemi della DOPPIA QUERY.
'''
import mysql.connector  # pip install mysql-connector-python
from helpers.File_writer import File_writer

def compareChosenDevicesByAlarmPriority(cursor):
    #chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84',
    #                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
    # our devices: ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    chosenDevices = ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']

    for d in chosenDevices:
        fw = File_writer(d, "afterNoDup")
        fw.create_txt("../../res/newres/")
        markedEvents = []
        print '\nDEVICE '+ str(d) + ': '
        fw.write_txt('\nDEVICE '+ str(d) + ': ')
        for l in levelsOfPriority:
            print '\n\tPRIORITY ' + str(l) + ':'
            fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
            query = ("select * from electric where device=%s and livellopriorita=%s and action='Alarm CAME'")
            cursor.execute(query, (d,l))
            events = cursor.fetchall()
            afterSeq = [] # Contiene le liste dei device che vediamo in ogni riga nei file di testo
            afterSequence = [] # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste

            for e in events:
                query = ("select * from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
                cursor.execute(query, (e[0], e[0], 5))
                eventsAfter = cursor.fetchall()
                devicesAfter = []  # all events that happened 5 min after the event "e"
                for ea in eventsAfter:
                    if ea not in markedEvents:
                        markedEvents.append(ea)
                        devicesAfter.append(ea[4]) #ea[4] = nome del device
                afterSequence.append(devicesAfter) # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
                devicesAfter=list(set(devicesAfter)) #Lista non ordinata di distinct devices
                afterSeq.append(devicesAfter) #Lista di liste (i.e. tutti quello dopo "distinct device after 5 min")
            
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
            
            print("==>")
            fw.write_txt('==>', newline = True) #KEEP THIS!


cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
cursor = cnx.cursor()
compareChosenDevicesByAlarmPriority(cursor)
