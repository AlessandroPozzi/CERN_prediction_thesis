
# coding: utf-8

# In[7]:


from datetime import datetime
import os
import json
from blaze.expr.datetime import DateTime

def loadGlitchesFromFile(filename):
    glitches = json.load(open(filename))
    glitches  = {float(k):v for k,v in glitches.items()}
    stopBeam = []
    notStopBeam =[]
    stop_temp=dict()
    nostop_temp=dict()
    for item in glitches:
        if glitches[item][1] == True:
            stop_temp[item] = glitches[item]
        else:
            nostop_temp[item] = glitches[item]

    for k in stop_temp.items():
        stopBeam.append(k[0])
 
    for n in nostop_temp.items():
       notStopBeam.append(n[0])
    return [stopBeam, notStopBeam]



# In[8]:


[stop, nonStop] = loadGlitchesFromFile("glitchesDB.txt")


# In[9]:

for unixTS in stop:
    print(datetime.fromtimestamp(unixTS))

print(str(len(stop)))
print(stop)

