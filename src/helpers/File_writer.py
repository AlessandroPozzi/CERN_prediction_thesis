'''
Created on 27 dic 2017

@author: Alessandro Corsair
'''
from __builtin__ import str

class File_writer(object):
    '''
    Class used to write in files 
    '''
    def __init__(self, *args):
        ''' Creates a File_writer object. Each argument you pass will be concatenated 
        with a "_" and will compose the name of the file '''
        self.file_name =  "_".join(args)
        self.file_name = self.file_name.replace("/", "")
        self.file_name = self.file_name.replace("*", "")
        self.created = False

    def create_txt(self, dir = "../../output/"):
        ''' Creates the file in the directory specified (by default is: "../output/" ) '''
        self.path = dir + self.file_name
        self.txt = open(self.path + ".txt", "w")
        self.txt.close()
        self.created = True
        
    def write_txt(self, text, newline = False):
        ''' Appends the text given. "Newline" creates a new line BEFORE the text, if True. 
        Newline AFTER the text is ALWAYS created. '''
        
        self.check_created()
        if not isinstance(text, str):
            text = str(text)
        self.txt = open(self.path + ".txt", "a")
        if newline:
            self.txt.write("\n")
        self.txt.write(text)
        self.txt.write('\n') #newline after the text
        self.txt.close()
        
    def write_inline(self, text):
        ''' Writes the text in the same line '''
        self.check_created()
        if not isinstance(text, str):
            text = str(text)
        self.txt = open(self.path + ".txt", "a")
        self.txt.write(text)
        self.txt.close()

    def write_list(self, lst, newline = False):
        ''' Pretty writes a list in the txt file '''
        self.check_created()
        self.txt = open(self.path + ".txt", "a")
        if newline:
            self.txt.write("\n")
        for e in lst:
            self.txt.write(str(e))
            self.txt.write("\n")
            
    def check_created(self):
        ''' Creates the txt file, if it has not been created yet '''
        if not self.created:
            self.create_txt()
        