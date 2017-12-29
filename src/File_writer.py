'''
Created on 27 dic 2017

@author: Alessandro Corsair
'''
from __builtin__ import str

class File_writer(object):
    '''
    Class used to write in files 
    '''

    def __init__(self, device, priority):
        '''
        Constructor
        '''
        self.device = device
        self.priority = priority
        self.path = '../output/' + self.device + "_" + self.priority
        self.txt = open(self.path + ".txt", "w")
        self.txt.close()
        
        
    def write_txt(self, text, newline = False):
        ''' Appends the text given. "Newline" creates a new line BEFORE the text, if True. 
        Newline AFTER the text is ALWAYS created. '''
        
        if not isinstance(text, str):
            text = str(text)
        self.txt = open(self.path + ".txt", "a")
        if newline:
            self.txt.write("\n")
        self.txt.write(text)
        self.txt.write('\n') #newline after the text
        self.txt.close()
