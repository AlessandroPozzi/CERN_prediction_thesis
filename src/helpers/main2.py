'''
Created on 28 dic 2017
@author: Alessandro Corsair

This is a (probably only temporary) module that can be used to do some tests.
'''
import graphviz as gv
from datetime import datetime
from datetime import timedelta
import numpy as np





'''
mydate = "2016-01-10 23:49:21.678"
mydate2 = "2016-01-10 23:43:59.741"


date1 = datetime.strptime(mydate, '%Y-%m-%d %H:%M:%S.%f')
date2 = datetime.strptime(mydate2, '%Y-%m-%d %H:%M:%S.%f')
time5min = timedelta(seconds = 5.40)
print(str(time5min.microseconds / 1000))

myArray = [[1], [2], [3]]
a = np.array(myArray)
print(a.shape)
'''
'''
m = np.matrix([[1,2,3,4], [2,2,2,2], [1,1,1,1]])
s = np.matrix([])
print(s.shape[1])
if s.shape[1] == 0:
    print("OK!")
'''
'''
print(date1 - date2)
if (date1-date2) < time5min:
    print("less than 5 min")
else:
    print("more than 5 min")

ls = ["A", "B", "C", "D"]
print(ls[2:4])
print(ls[0:2])
print(ls[0:1])
'''

'''
g = gv.Digraph(format='png')

    


g1 = gv.Digraph(name = "cluster_1")
g1.node("nodeA")
g1.node("nodeB")
g1.graph_attr['label']="nome"

g2 = gv.Digraph(name = "cluster_2")
g2.node("node1", label="a fantastic label", xlabel = "an external label")
g2.node("node2")
g2.node("node3")
g2.edge("node1", "node2", color= "red")

g.subgraph(g1)
g.subgraph(g2)
g.edge("nodeA", "node2")


g.render("test_graphviz", "../output/")

log_extractor = Log_extractor()

#log_extractor.extract_raw_data() #RUN THIS ONLY IF YOU STILL DON'T HAVE THE UNIQUE 2016 .CSV
file_devices = ["EMC001*9", 'EHS60/BE', 'ESS11/5H', 'ESS1*84', 'EXS4/8X', 'EXS106/2X']

#d = log_extractor.count_occurrences(file_devices)
#print(d)
'''
'''
device_occurrences = log_extractor.findtop_occurrences()
with open('../output/alldevices_occurrences.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(["Device", "Occurrences"])
    for devocc in device_occurrences:
        writer.writerow([devocc[0], devocc[1]])
    
print(device_occurrences)
'''
