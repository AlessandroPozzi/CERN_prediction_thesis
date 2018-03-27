'''
USA questo file solo per generare grafici su priorita'

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
import matplotlib.pyplot as plt
import numpy as np

def compareChosenDevicesByAlarmPriority(cursor):
    chosenDevices = config.chosenDevices
    levelsOfPriority = config.levelsOfPriority
    minutes = config.CORRELATION_MINUTES
    fig = plt.figure(figsize=(11,8))
    ax1 = fig.add_subplot(111)
    
    x = [0,1,2,3,4,5,6,7,8,9,10]
    lb = 0
    ally = []
    for d in chosenDevices:
        y = [0]
        for t in [1,2,3,4,5,6,7,8,9,10]:
            minutes = t
            print("Minutes = " + str(minutes))
            fw = File_writer(d, config.FILE_SUFFIX, config.EXTRA)
            fw.create_txt("../../res/")
            print '\nDEVICE '+ str(d) + ': '
            fw.write_txt('\nDEVICE '+ str(d) + ': ')
            lb += 1
            totMarked = 0
            for l in levelsOfPriority:
                markedEvents = []
                print '\n\tPRIORITY ' + str(l) + ':'
                fw.write_txt('\n\tPRIORITY ' + str(l) + ':')
                query = ("select * from electric where device=%s and livellopriorita=%s and action='Alarm CAME' order by time")
                cursor.execute(query, (d,l))
                events = cursor.fetchall()
                afterSeq = [] # Contiene le liste dei device che vediamo in ogni riga nei file di testo
                afterSequence = [] # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
    
                for e in events:
                    query = ("select * from electric where time>=(%s) and time <= (%s + interval %s minute) and action='Alarm CAME' order by time;")
                    cursor.execute(query, (e[0], e[0], minutes))
                    eventsAfter = cursor.fetchall()
                    devicesAfter = []  # all events that happened 5 min after the event "e"
                    for ea in eventsAfter:
                        if ea not in markedEvents: #CONDIZIONE per rimuovere i DUPLICATI
                            markedEvents.append(ea)
                            if ea[4] != d: #CONDIZIONE per evitare problemi con il device di riferimento e l'aggiunta di stati, tag o descr.
                                if config.EXTRA == "state":
                                    index = 10
                                elif config.EXTRA == "tag":
                                    index = 5
                                elif config.EXTRA == "description":
                                    index = 6
                                if config.EXTRA:
                                    extraColumn = ea[index].encode('ascii', 'ignore').decode('ascii')
                                    extraColumn = extraColumn.replace("'", "")
                                    #extraColumn = re.escape(extraColumn)
                                else:
                                    extraColumn = ""
                                devicesAfter.append(ea[4] + "--" + extraColumn)
                    #if devicesAfter != []:
                    afterSequence.append(devicesAfter) # Contiene tutte le liste di deviceAfter (con duplicati). E' una lista di liste
                    devicesAfter=list(set(devicesAfter)) #Lista non ordinata di distinct devices
                    afterSeq.append(devicesAfter) #Lista di liste (i.e. tutto quello dopo "distinct device after 5 min")
                
                '''
                #CONSOLE
                print '\n\t\tDistinct devices after 5 minutes: [ '
                for xx in afterSeq:
                    print '\t\t[ ',
                    for yy in xx:
                        print "'" + str(yy) + "', ",
                    print '], '
                print ']'
                '''
                    
                #NUMBER OF EVENTS
                print("Number of events: " + str(len(markedEvents)))
                totMarked += len(markedEvents)
                if l =='L3':
                    y.append(len(markedEvents))
                    #y.append(totMarked)
                
                #TEXT FILE
                fw.write_txt('\n\t\tDistinct devices after 5 minutes: [ ')
                for xx in afterSeq:
                    fw.write_inline( '\t\t[ ', )
                    for yy in xx:
                        fw.write_inline( "'" + str(yy) + "', " )
                    fw.write_txt('], ')
                fw.write_txt(']')
                
                if config.unitePriorities: #put everything in the same "L0" priority
                    pass
                else:
                    print("==>")
                    fw.write_txt('==>', newline = True) #KEEP THIS!
        ally.append(y)
            
    y_stack = np.cumsum(y, axis=0)
    lb=0
    maxY = 0
    for ydata in ally:
        #y_stack[lb,:]
        maxY = max(max(ydata), maxY)
        ax1.plot(x, ydata, label=config.chosenDevices[lb])
        lb += 1
    
    
    plt.xticks(np.arange(min(x), max(x)+1, 1.0))
    plt.grid()
    #colormap = plt.cm.gist_ncar
    colormap = plt.cm.get_cmap('Spectral')
    plt.ylim([0,maxY])
    colors = [colormap(i) for i in np.linspace(0, 1,len(ax1.lines))]
    for i,j in enumerate(ax1.lines):
        j.set_color(colors[i])
    ax1.legend(loc=2)
    plt.show()

def searchItemsets():
    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='password', database='cern')
    cursor = cnx.cursor()
    compareChosenDevicesByAlarmPriority(cursor)
    cursor.close()

if __name__ == "__main__":
    searchItemsets()