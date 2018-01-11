import mysql.connector  # pip install mysql-connector-python
from pymining import itemmining # pip install pymining  
from File_writer import File_writer

support = 0.5

levelOfTensionDictionaryLV = {
    'A': 'LV ASSURED BY DIESEL',
    'B': 'LV NORMAL',
    'C': 'ELV DIRECT CURRENT',
    'D': 'MACHINE MIXED SERVICES',
    'E': 'See Tab Naming HVA/HVB',
    'F': 'See Tab Naming HVA/HVB',
    'G': 'ELV DIRECT CURRENT',
    'H': 'See Tab Naming HVA/HVB',
    'I': 'RESERVED FOR NON EL EQUIPMENT',
    'J': 'LV DIRECT CURRENT',
    'K': 'See Tab Naming HVA/HVB',
    'L': 'Forbidden (prior authorisation required)',
    'M': 'See Tab Naming HVA/HVB',
    'N': 'SENSORS',
    'O': 'UPS NETWORK',
    'P': 'EMERGENCY LIGHTING',
    'Q': 'LV CRYOGENICS',
    'R': 'LV MACHINE POWER CONVERTERS',
    'S': 'LV SAFETY NETWORK',
    'T': 'See Tab Naming Supervision',
    'U': 'EMERGENCY STOP',
    'V': 'LV VACUUM',
    'W': 'LV COOLING AND VENTILATION',
    'X': 'LV EXPERIMENT',
    'Y': 'See Tab Naming Racks',
    'Z': 'LV RF',
}
levelOfTensionDictionaryHV = {
    'A': 'See Tab Naming LV',
    'B': 'See Tab Naming LV',
    'C': 'See Tab Naming LV',
    'D': 'Forbidden (prior authorisation required)',
    'E': 'HV  EDF 20 kV AC',
    'F': 'HV 18 kV  SAFETY NETWORK',
    'G': 'See Tab Naming LV',
    'H': 'HV TRANSPORT NETWORK 66/400 kV AC',
    'I': 'RESERVED FOR NON EL EQUIPMENT',
    'J': 'See Tab Naming LV',
    'K': 'HV CRYO/SAFETY 3.3 kV AC',
    'L': 'Forbidden (prior authorisation required)',
    'M': 'HV DISTRIBUTION/TRANSPORT 18 kV AC',
    'N': 'See Tab Naming LV',
    'O': 'See Tab Naming LV',
    'P': 'See Tab Naming LV',
    'Q': 'See Tab Naming LV',
    'R': 'See Tab Naming LV',
    'S': 'See Tab Naming LV',
    'T': 'See Tab Naming Supervision',
    'U': 'See Tab Naming LV',
    'V': 'See Tab Naming LV',
    'W': 'See Tab Naming LV',
    'X': 'See Tab Naming LV',
    'Y': 'See Tab Naming Racks',
    'Z': 'See Tab Naming LV',
}
levelOfTensionDictionaryRacks = {
    'B': 'LV PROTECTION',
    'C': 'EL CONTROL',
    'E': 'MEASUREMENT',
    'H': 'HV 66/400 kV PROTECTION',
    'K': 'HV 3.3 kV PROTECTION',
    'M': 'HV 18 kV PROTECTION',
    'N': 'RESERVED',
    'O': 'UPS NETWORK',
    'P': 'EMERGENCY LIGHTING',
    'Q': 'COMPENSATORS',
    'S': 'COMMUTATION NORMAL/SAFETY',
    'U': 'EMERGENCY STOP',
    'Y': 'SHARED RACK',
}
typeDictionaryLV = {
    'A': 'SOURCE TRANSFER SWITCH',
    'B': 'COMMAND (BUTTON RING)',
    'C': 'CRATE - CONTROL',
    'D': 'DISTRIBUTION (SWITCHBOARD FEEDER)',
    'E': 'ANALOGUE MEASURE',
    'F': 'Forbidden (prior authorisation required)',
    'G': 'GENERATOR',
    'H': 'PROTECTIVE RELAY',
    'I': 'CRATE - INTERCONNEXION-SYNCHRO',
    'J': 'DISTRIBUTION BOX',
    'K': 'TRUNKING SYSTEMS',
    'L': 'LIGHTING CIRCUIT, LOCAL EPO',
    'M': 'ELECTRICAL MOTOR',
    'N': 'Forbidden (prior authorisation required)',
    'O': 'Forbidden (prior authorisation required)',
    'P': 'POWER CIRCUIT - SOCKET OUTLET',
    'Q': 'COMPENSATORS',
    'R': 'WATER HEATER CIRCUIT',
    'S': 'UPS',
    'T': 'TRANSFORMER',
    'U': 'CHARGER / RECTIFIER / INVERTER',
    'V': 'VENTILATION EXTRACTION CIRCUIT',
    'W': 'Forbidden (prior authorisation required)',
    'X': 'JUNCTION BOX / CABLE JOINT',
    'Y': 'Forbidden (prior authorisation required)',
    'Z': 'Forbidden (prior authorisation required)',
}
typeDictionaryHV = {
    'A': 'CURRENT TRANSFORMER',
    'B': 'BUSBAR',
    'C': 'CRATE - CONTROL/COMMAND',
    'D': 'DISTRIBUTION SWITCHBOARD',
    'E': 'CRATE -  MEASURE OF TENSION',
    'F': 'FILTER',
    'G': 'GENERATOR',
    'H': 'PROTECTIVE RELAY',
    'I': 'TRANSFO BOX',
    'J': 'DISTRIBUTION CRATE (MEASURE OF TENSION)',
    'K': 'CABLE SEGMENTS JUNCTION',
    'L': '****',
    'M': 'ELECTRICAL MOTORS',
    'N': 'Forbidden (prior authorisation required)',
    'O': 'Special (prior authorisation required)',
    'P': 'Forbidden (prior authorisation required)',
    'Q': 'REACTANCE',
    'R': 'RESISTOR',
    'S': 'DISCONNECTOR',
    'T': 'TRANSFORMER',
    'U': 'EXCITATION',
    'V': 'TRANSFORMER FAN/GENERATOR COMPRESSOR',
    'W': 'VOLTAGE TRANSFORMER',
    'X': 'EARTHING SWITCH',
    'Y': 'Forbidden (prior authorization required)',
    'Z': 'SURGE ARRESTER',
}

# for each device the function derives five more attributes related to the device and obtained
# by following the naming convention provided by CERN
def expandDeviceMeaning(cursor):
    f = open("..res/CERNdevices.txt", 'r')
    male=1
    for line in f:
        print male
        male = male+1
        one = ''
        two = ''
        three = ''
        four = ''
        five = ''
        six = ''

        line = line[0:-1] # delete last character which is a newline
        if line[0:4] == "UPS_":
            three="UPS"
        one = line[0]
        if one == 'E':
            two = levelOfTensionDictionaryLV.get(line[1], "*** NOT IN TABLES ***")
            three = typeDictionaryLV.get(line[2], "*** NOT IN TABLES ***")
            if two == "See Tab Naming HVA/HVB":
                two = levelOfTensionDictionaryHV.get(line[1], "*** NOT IN TABLES ***")
                three = typeDictionaryHV.get(line[2], "*** NOT IN TABLES ***")
            if two == "See Tab Naming Racks":
                two = levelOfTensionDictionaryRacks.get(line[2], "*** NOT IN TABLES ***")
                three = "RACK"

            i=3
            while i<len(line) and line[i]!='/' and line[i]!='*':
                four = four + line[i]
                i = i+1

            if i<len(line) and line[i] == '/':
                if '0' <= line[i+1] <= '9':
                    five = "LHC"
                else:
                    five ="SPS"
            elif i<len(line) and line[i]=='*':
                five = "Meyrin"
            else:
                five = "*** NOT IN TABLES ***"
            i=i+1
            while i< len(line):
                six = six + line[i]
                i = i+1

        print line + ': ' + one + '; ' + two + '; ' + three + '; ' + four + '; ' + five + '; ' + six

        query = ('update electric set Device_FirstLetter=%s, Device_SecondLetter_LevelOfTension=%s, Device_ThirdLetter_Type=%s, Device_Fourth_Number=%s, Device_Fifth_LHCspsMeyrin=%s, Device_Sixth_Position=%s where Device=%s;')
        cursor.execute(query, (one, two, three, four, five, six, line))
        cnx.commit()

# for each distinct value of the alarm priority, the function computes the average length
# of the sequences or events that happened after the considered event
def SequenceLengthPriorityByMinutes(cursor):
    for alarmThreshold in [126, 124, 116, 101, 100, 99, 95, 90, 86, 80, 70, 65, 61, 60, 59, 56, 50, 40, 36, 35, 30, 20]:
        print 'AlarmThreshold: ' + str(alarmThreshold),
        query = ('select * from electric where AlarmPriority=%s;')
        cursor.execute(query, (alarmThreshold,))
        allAlarms = cursor.fetchall()
        howManyAlarms = 1.0 * allAlarms.__len__()
        print 'ci sono ' + str(howManyAlarms) + ' alarms con priorita uguale a ' + str(alarmThreshold)
        for minutes in range(1, 6):
            print 'Minutes: ' + str(minutes),
            sum=0.0
            for alarm in allAlarms:
                #print 'alarm'
                query = ('select count(*) from electric where AlarmPriority<=20 and time>=(%s) and time <= (%s + interval %s minute);')
                cursor.execute(query, (alarm[0], alarm[0], minutes))
                l = cursor.fetchone()[0]
                #print 'allarme lungo ' + str(l)
                sum = sum + l
            average = sum / howManyAlarms
            print 'Alarm Threshold: ' + str(alarmThreshold) + ', Minutes: ' + str(minutes) + '--> averageLength: ' + str(average)

#
def compareChosenDevicesByAlarmPriority(cursor):
    #chosenDevices = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84',
    #                 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']
    # our devices: ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    chosenDevices = ['EMC001*9', 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']
    levelsOfPriority = ['L0', 'L1', 'L2', 'L3']


    for d in chosenDevices:
        fw = File_writer(d, "ba")
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

            for e in events:
                #print '\n' + str(e)
                query = ("select * from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
                cursor.execute(query, (e[0], e[0], 5))
                eventsAfter = cursor.fetchall()
                devicesAfter = []  # all events that happened 5 min after the event "e"
                for ea in eventsAfter:
                    devicesAfter.append(ea[4]) #ea[4] = nome del device
                afterSequence.append(devicesAfter) # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
                devicesAfter=list(set(devicesAfter)) #Lista non ordinata di distinct devices
                afterSeq.append(devicesAfter) #Lista di liste (i.e. tutti quello dopo "distinct device after 5 min")

                #print '--- Devices after: ' + str(devicesAfter.__len__()) + ': ' + str(devicesAfter)
                
                query = ("select * from electric where time<=(%s) and time >= (%s - interval %s minute) and action='Alarm CAME';")
                cursor.execute(query, (e[0], e[0], 5))
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


'''
al = 1
alarmThreshold = int(raw_input("Set the alarm threshold: "))
minutesThreshold = int(raw_input("Set the minutes threshold: "))
filename = 'sequencesAlarm' + str(alarmThreshold) + 'Minutes' + str(minutesThreshold) + '.txt'
f = open(filename, 'w')

# find all alarms with priority greater than the threshold
query = ('select * from electric where AlarmPriority>=%s order by time;')
cursor.execute(query, (alarmThreshold,))
allAlarms = cursor.fetchall()

# find all devices that had at least one alarm event
allDevices=[]
for a in allAlarms:
    allDevices.append(a[4])
allDevices = list(set(allDevices))
f.write(str(allDevices.__len__()) + ' device sono stati in allarme')

#dev = ['EHS60/BE', 'EXS4/8X', 'EMC001*9', 'EXS106/2X', 'ESS1*84', 'ESS11/5H', 'ESS406/E91', 'ESS407/E91', 'ESS520/E91', 'ESS11*84']

i=1
# consider each device that had an alarm event
for dev in allDevices:
    # find the alarms the device had
    query = ('select * from electric where Device=%s and AlarmPriority>=%s order by time;')
    cursor.execute(query, (dev,alarmThreshold,))
    allAlarms=cursor.fetchall()
    nrAlarms = allAlarms.__len__()
    print 'device ' + str(i) + ' ' + str(dev)
    f.write('\n# device ' + str(i) + ': ' + str(dev) + ' had ' + str(nrAlarms) + ' alarms: [')
    i=i+1

    #print '(',
    print '[',
    # consider each alarm the device had and find what happened in the following MINUTESTHRESHOLD minutes
    for alarm in allAlarms:
        #print str(al) + ") --> " + str(alarm[0]) + ', ' + str(alarm[4]) + ': ' + str(alarm[1:])
        previous = alarm[0]
        time = alarm[0]
        device = alarm[4]

        query = ('select * from electric where time>=(%s) and time <= (%s + interval %s minute);')
        cursor.execute(query, (time, time, minutesThreshold,))
        allresults = cursor.fetchall()
        f.write('\n  * [')

        qqq = (query, (time, time, minutesThreshold,))
        #print str (qqq)
        #print allresults.__len__()

        print '[',
        involvedDevices=[]
        #print '\n'
        for c in allresults[:-1]:
            print "'" + str(c[4]) + "',",
        #    print str(c[0]) + ", " + str(c[0] - previous) + ", " + str(c[1:])
            involvedDevices.append(c[4])
            f.write(str(c[4]) + ',')

        c = allresults[allresults.__len__()-1]
        #print "'" + str(c[4]) + "'",
        # print str(c[0]) + ", " + str(c[0] - previous) + ", " + str(c[4])
        involvedDevices.append(c[4])
        f.write(str(c[4]) + ',')

        f.write(']')
        print '],'

        involvedDevices = list(set(involvedDevices))
        #print '(',
        #for inv in involvedDevices[:-1]:
        #    print "'" + str(inv) + "',",
        #inv=involvedDevices[involvedDevices.__len__()-1]
        #print "'" + str(inv) + "' ),"

        f.write('\n\t\t---> Involved Devices: ' + str(involvedDevices))

    #print ')'
    print ']'
    f.write(']')


f.close()
'''