# USE THIS TO CHECK IF STATES, TAGS AND DESCRIPTIONS ARE MEANINFUL

import mysql.connector  # pip install mysql-connector-python
from pymining import itemmining # pip install pymining  
from File_writer import File_writer


def compareChosenDevicesByAlarmPriority(cursor):
    #chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84',
    #                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
    # our devices: ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    device_filtering = ["EHT2/BE", "EHT1/BE", "ECE001/BE", "EHT4/BE", 
                        "EHT3/BE", "EHH501/9E", "EHT5/BE", "EHD20/BE"] #USE THIS LIST TO SHOW ONLY SOME SPECIFIC DEVICES
                                                                        #NOTA: COPIA E INCOLLA LA LISTA CHE VIENE GENERATA NEI TXT DELLE RETI
    chosenDevices = ['EHS60/BE']
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']


    for d in chosenDevices:
        fw = File_writer(d, "+state")
        fw.create_txt("../res/newres/")
        print '\nDEVICE '+ str(d) + ': '
        fw.write_txt('\nDEVICE '+ str(d) + ': ')
        for l in levelsOfPriority:
            print '\n\tPRIORITY ' + str(l) + ':'
            fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
            query = ("select * from electric where device=%s and livellopriorita=%s and action='Alarm CAME'")
            #query = ("select * from electric E1 where device=%s and livellopriorita=%s and not exists (select * from electric E2 where E2.id<>E1.id and (livellopriorita='L1' or livellopriorita='L2') and E2.time<=E1.time and E2.time>=(E1.time - interval %s minute));")
            cursor.execute(query, (d,l))
            events = cursor.fetchall()
            afterSeq = [] # Contiene le liste dei device che vediamo in ogni riga nei file di testo
            beforeSeq = []
            afterSequence = [] # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
            beforeSequence = []
            intersectionDevicesBeforeAndAfter = []
            statesSet = []
            
            for e in events:
                #print '\n' + str(e)
                query = ("select Device, Time, State, Tag, Description from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
                cursor.execute(query, (e[0], e[0], 5))
                eventsAfter = cursor.fetchall()
                devicesAfter = []  # all events that happened 5 min after the event "e"
                
                for ea in eventsAfter:
                    #devicesAfter.append(ea[0]) #ea[4] = nome del device
                    if ea[0] in device_filtering or ea[0] == d: #to show directly the devices of the newtork
                        devicesAfter.append((ea[0], ea[2]))
                        statesSet.append(ea[2])
                afterSequence.append(devicesAfter) # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
                devicesAfter=list(set(devicesAfter)) #Lista non ordinata di distinct devices
                afterSeq.append(devicesAfter) #Lista di liste (i.e. tutti quello dopo "distinct device after 5 min")
                statesSet = list(set(statesSet)) #TO CHECK THE STATES EASILY
                #print '--- Devices after: ' + str(devicesAfter.__len__()) + ': ' + str(devicesAfter)
                
                query = ("select * from electric where time<=(%s) and time >= (%s - interval %s minute) and action='Alarm CAME';")
                cursor.execute(query, (e[0], e[0], 2))
                eventsBefore = cursor.fetchall()
                devicesBefore=[]
                for eb in eventsBefore:
                    devicesBefore.append(eb[4])
                beforeSequence.append(devicesBefore)
                devicesBefore = list(set(devicesBefore))
                beforeSeq.append(devicesBefore)

                #print '--- Devices before: ' + str(devicesBefore.__len__()) + ': ' + str(devicesBefore)

                intersect = set(devicesBefore).intersection(set(devicesAfter))
                intersectionDevicesBeforeAndAfter.append(intersect)

                #print '--- Intersection of devices after and devices before: ' + str(intersect)

            #relim_input = itemmining.get_relim_input(intersectionDevicesBeforeAndAfter)
            #report = itemmining.relim(relim_input, min_support=int(intersectionDevicesBeforeAndAfter.__len__() * support))
            #print "\n\n\t\t  Frequent Itemsets in Intersection: (soglia=" + str(intersectionDevicesBeforeAndAfter.__len__() * support) + ")"
            #for key, values in report.items():
            #    print '\t\t\t',
            #    print key,
            #    print ': ',
            #    print values

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
            
            for s in statesSet:
                s = s.encode('ascii', 'ignore').decode('ascii')
                fw.write_txt(s)
            fw.write_txt("Different element (states, tag or descr) in this priority: " + str(len(statesSet)))
            
            
            print("==>")
            fw.write_txt('==>', newline = True) #KEEP THIS ONLY IF YOU ARE NOT GENERATING THE FREQUENT SETS
            
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

#expandDeviceMeaning(cursor)

compareChosenDevicesByAlarmPriority(cursor)
