'''
Created on 31 mar 2018

@author: Alessandro Corsair
'''
import matplotlib.pyplot as plt
import DataError
import numpy as np

class Graph_drawer:
    '''
    Draws simple graphs with multiple traces
    '''
    
    def __init__(self, x = [], autoMax = None, start = 0, step = 0.5):
        if autoMax == None:
            self.x = x
        else:
            self.x = self.createRange(start, step, autoMax)
        self.fig = plt.figure(figsize=(11,8))
        self.ax1 = self.fig.add_subplot(111)
        self.yData = []
        self.names = []
    
    def createRange(self, x, step, end):
        finalList = []
        while x <= end:
            finalList.append(x)
            x += step
        return finalList
    
    def addTrace(self, y, name):
        '''
        y = list of data points 
        name = name of this trace 
        '''
        if len(y) != len(self.x):
            raise DataError("The data points given as 'y' must match the number of data points of 'x'")
            return
        self.yData.append(y)
        self.names.append(name)
        
    def draw(self):
        maxY = 0
        for x, y, name in zip(self.x, self.ydata, self.names):
            #y_stack[lb,:]
            maxY = max(max(y), maxY)
            self.ax1.plot(x, y, label=name)
        plt.xticks(np.arange(min(self.x), max(self.x)+(max(self.x)*0.1), 0.5))
        plt.grid()
        
        colormap = plt.cm.get_cmap('Spectral')
        plt.ylim([0,(maxY+100)])
        colors = [colormap(i) for i in np.linspace(0, 1,len(self.ax1.lines))]
        for i,j in enumerate(self.ax1.lines):
            j.set_color(colors[i])
        self.ax1.legend(loc=2)
        plt.show()
            
            
            
            
            