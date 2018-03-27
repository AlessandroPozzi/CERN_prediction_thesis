import numpy as np
import mysql.connector  # pip install mysql-connector-python
import os
import config
import re
import pomegranate as pomgr
import graphviz as gv
import expandDeviceMarkov
from DataError import DataError
import string

class MarkovHandler:
    '''
    Handles creation and usage of the probabilistic network over CERN's data.
    Can deal only with a SINGLE file-priority combination.
    Note that the methods of this class have numbers and must be called in order.
    '''

    def __init__(self, pnh, gh):
        '''
        Constructor
        '''
        extractor = pnh.get_data_extractor()
        self.training_instances = ""
        self.device_considered = pnh.get_device()
        self.device_considered_realName = pnh.get_device_realName()
        self.priority_considered = pnh.get_priority()
        self.general_handler = gh
        self.variables_names = extractor.get_variable_names()
        self.rankedDevices = extractor.get_ranked_devices()
        self.file_writer = pnh.get_file_writer()
        self.file_suffix = pnh.get_file_suffix()
        self.devicesColumnDict = pnh.get_column_analysis()
        self.occurrences = pnh.get_occurrences()

    def create_mc_model(self, seqWithPriority):
        sequences = self.getSequencesOfPriority(seqWithPriority)
        seqWithDevConsideredOnly = self.getOnlyDevConsidered(sequences)
        (self.mc, self.avg_var_list, self.ref_dev_avg_vars) = expandDeviceMarkov.create_mc_model(self.device_considered_realName,
            self.priority_considered, self.variables_names, seqWithDevConsideredOnly)

    def draw_mc_model(self, location_choice, info_choice, avg_var_edges, refDevice, hideNames, onlyH0):
        if location_choice and info_choice:
            DataError("location_choice and info_choice can't be both True")
            return
        mc_graph = gv.Digraph(format="png")
        mc_graph.graph_attr['overlap'] = "false"
        mc_graph.graph_attr['rankdir'] = 'LR'
        devicesExtraString = self.variables_names #format string: "device--extra"
        devicesExtraCouple = [] #format couple: (device, extra)
        for de in devicesExtraString:
            devName = re.compile('(.*?)\-\-').findall(de)
            extraName = re.compile('\-\-(.*?)\Z').findall(de)
            devicesExtraCouple.append((devName[0],extraName[0]))
        # Automatically change the names of the nodes
        if hideNames:
            #inside this method also the reference device is converted
            realFakeNamesDict = self.convertNamesCaesarCipher(devicesExtraString, 5)

        # Create subgraph for the locations
        if location_choice:
            device_location = dict()
            device_locationH1 = dict()
            allLocH0 = []
            allLocH1 = []

            # For H0 and H1:
            allDevicesLocations = self.general_handler.get_device_locations() #key=device name (no extra!)
            for de in devicesExtraCouple:
                device_location[de[0]] = allDevicesLocations[de[0]][0] #H0
                allLocH0.append(device_location[de[0]])
                device_locationH1[de[0]] = allDevicesLocations[de[0]][1] #H1
                allLocH1.append(device_locationH1[de[0]])
            if refDevice:
                ref = self.device_considered_realName
                device_location[ref] = allDevicesLocations[ref][0] #H0
                allLocH0.append(device_location[ref])
                device_locationH1[ref] = allDevicesLocations[ref][1] #H1
                allLocH1.append(device_locationH1[ref])
            location_color = self.assign_color(device_location)
            location_colorH1 = self.assign_color(device_locationH1)
            
            # Hide the locations names, if requested:
            realFakeLocationsH0Dict = self.convertNames(allLocH0, "Location")
            realFakeLocationsH1Dict = self.convertNames(allLocH1, "Location")

            # Creating the subgraphs, one for each location:
            loc_subgraphs = dict()
            for loc in location_color:
                name = "cluster_" + loc  #The subgraph name MUST start with "cluster_" 
                loc_subgraphs[loc] = gv.Digraph(name)
                if hideNames:
                    locatName = realFakeLocationsH0Dict[loc]
                else:
                    locatName = loc
                loc_subgraphs[loc].graph_attr['label'] = locatName  # Label with name to be visualized in the image
                loc_subgraphs[loc].graph_attr['overlap'] = "false"
                
        # Create subgraphs for occurrences, average and standard deviation:
        if info_choice:
            info_subgraphs = dict()
            # there's one subgraph for each node:
            id = 0
            for de in devicesExtraString:
                name = "cluster_" + str(id)  #The subgraph name MUST start with "cluster_" 
                id += 1
                '''
                if not config.EXTRA and "--" in de: 
                    deClean, _ = de.split("--")
                else:
                    deClean = de
                '''
                info_subgraphs[de] = gv.Digraph(name)
                label = "Occurrences: " + str(round(self.occurrences[de], 2)) #+ " | "
                #label = label + "Avg: " + str(round(self.devicesColumnDict[de].msAverage / 1000, 2)) + "s\n"
                #label = label + "St.Dev.: " + str(round(self.devicesColumnDict[de].msStandDev / 1000, 2)) + "s"
                info_subgraphs[de].graph_attr['label'] = label  # Label with name to be visualized in the image
                info_subgraphs[de].graph_attr['overlap'] = 'scale'

        # Create nodes. Comment this part if you want to eliminate nodes without edges. This has been done to send
        # networks to CERN
        for de in devicesExtraCouple:
            nodeName = de[0] + "--" + de[1]
            devName = de[0]
            if hideNames:
                nodeName2 = realFakeNamesDict[nodeName]
            else:
                nodeName2 = nodeName
            if not config.EXTRA and "--" in nodeName2:
                #remove the "--" from the name since there is no extra
                nodeName3, _ = nodeName2.split("--")
            else:
                nodeName3 = nodeName2
            if location_choice:
                locationH0 = device_location[devName]
                locationH1 = device_locationH1[devName]
                if not onlyH0:
                    loc_subgraphs[locationH0].node(nodeName3, style='filled',
                                                fillcolor=location_colorH1[locationH1]) #add the node to the right subgraph
                else: # Add only the H0 locations
                    loc_subgraphs[locationH0].node(nodeName3)
                # loc_subgraphs[locationH0].node(node) #USE THIS TO ADD ONLY H0
            elif info_choice:
                info_subgraphs[nodeName].node(nodeName3)
            else:  # add the node directly to the graph
                mc_graph.node(nodeName3)

        # Reference device
        if refDevice and location_choice:
            ref = self.device_considered_realName
            locationH0 = device_location[ref]
            locationH1 = device_locationH1[ref]
            if hideNames:
                nodeName = realFakeNamesDict[ref]
            else:
                nodeName = ref
            if not onlyH0:
                loc_subgraphs[locationH0].node(nodeName, style='filled',
                                                fillcolor=location_colorH1[locationH1]) #add the node to the right subgraph
            else: # Add only the H0 location
                loc_subgraphs[locationH0].node(nodeName)
        elif refDevice:
            ref = self.device_considered_realName
            if hideNames:
                mc_graph.node(realFakeNamesDict[ref])
            else:
                mc_graph.node(ref)

        # Add all subgraphs in the final graph:
        if location_choice:
            for loc in loc_subgraphs:
                mc_graph.subgraph(loc_subgraphs[loc])
        elif info_choice:
            for dev in info_subgraphs:
                mc_graph.subgraph(info_subgraphs[dev])

        # Legend for color:
        if location_choice and not onlyH0:
            legend = ""
            for loc in location_colorH1:
                if hideNames:
                    legend = legend + location_colorH1[loc] + " --> " + realFakeLocationsH1Dict[loc] + "\n"
                else:
                    legend = legend + location_colorH1[loc] + " --> " + loc + "\n"
            mc_graph.node(legend, shape="box")

        # Create edges
        cpt = self.mc.distributions[1].parameters[0]
        drawnEdges = {}
        for i in range(0, len(cpt)):
            edge0 = cpt[i][0]
            edge1 = cpt[i][1]

            if hideNames:
                edge0 = realFakeNamesDict[edge0]
                edge1 = realFakeNamesDict[edge1]

            prob = round(cpt[i][2], 2)
            
            if (prob <= 0.25 and prob >= 0.0):
                #mc_graph.edge(edge0, edge1, color="Grey")
                pass
            else:
                if avg_var_edges:
                    if not config.EXTRA and "--" in edge0:
                        # remove the "--" from the name since there is no extra
                        edgeDraw0, _ = edge0.split("--")
                    else:
                        edgeDraw0 = edge0
                    if not config.EXTRA and "--" in edge1:
                        # remove the "--" from the name since there is no extra
                        edgeDraw1, _ = edge1.split("--")
                    else:
                        edgeDraw1 = edge1

                    avg, var = self.find_avg_var(edge0, edge1, hideNames, False)
                    if(avg == None or var == None):
                        lab = str(prob)
                    else:
                        lab = str(prob) + " | (" + str(avg) + "," + str(var) + ")"
                else:
                    lab = str(prob)
                    if not config.EXTRA and "--" in edge0:
                        # remove the "--" from the name since there is no extra
                        edgeDraw0, _ = edge0.split("--")
                    else:
                        edgeDraw0 = edge0
                    if not config.EXTRA and "--" in edge1:
                        # remove the "--" from the name since there is no extra
                        edgeDraw1, _ = edge1.split("--")
                    else:
                        edgeDraw1 = edge1

                if (edgeDraw1, edgeDraw0) in drawnEdges:
                    #oldLink rewriting
                    oldLink = (edgeDraw1, edgeDraw0)
                    oldLab = drawnEdges[oldLink]
                    if (prob > 0.25 and prob < 0.5):
                        mc_graph.edge(edgeDraw1, edgeDraw0, color="black", label=oldLab, portPos="nw", style="invis")
                    elif (prob >= 0.5):
                        mc_graph.edge(edgeDraw1, edgeDraw0, color="red", label=oldLab, portPos="nw", style="invis")
                    #new link in opposite direction
                    if (prob > 0.25 and prob < 0.5):
                        mc_graph.edge(edgeDraw0, edgeDraw1, color="black", label=lab, portPos="se")
                    elif (prob >= 0.5):
                        mc_graph.edge(edgeDraw0, edgeDraw1, color="red", label=lab, portPos="se")
                else:
                    if (prob > 0.25 and prob < 0.5):
                        mc_graph.edge(edgeDraw0, edgeDraw1, color="black", label=lab)
                    elif (prob >= 0.5):
                        mc_graph.edge(edgeDraw0, edgeDraw1, color="red", label=lab)
                #it is in drawnEdges only if prob > 0.33
                drawnEdges[(edgeDraw0, edgeDraw1)] = lab

        if refDevice:
            devProbList = self.mc.distributions[0].parameters
            for i in range(0, len(devProbList)):
                for node in devProbList[i]:
                    prob = round(devProbList[i][node], 2)
                    if hideNames:
                        otherNode = realFakeNamesDict[node]
                        refNode = realFakeNamesDict[self.device_considered_realName]
                    else:
                        otherNode = node
                        refNode = self.device_considered_realName
                    if prob > 0.15:
                        if not config.EXTRA and "--" in node:
                            # remove the "--" from the name since there is no extra
                            nodeToDraw, _ = otherNode.split("--")
                        else:
                            nodeToDraw = otherNode
                        if avg_var_edges:
                            avg, var = self.find_avg_var(refNode, otherNode, hideNames, True)
                            if (avg == None or var == None):
                                lab = str(prob)
                            else:
                                lab = str(prob) + " | (" + str(avg) + "," + str(var) + ")"
                        else:
                            lab = str(prob)
                        mc_graph.edge(refNode, nodeToDraw, color="blue", label=lab)

        # save the .png graph
        if location_choice:
            locat = "_H0H1"
        else:
            locat = ""
        if info_choice:
            info = "_Info"
        else:
            info = ""
        if avg_var_edges:
            avg_var = "_Var"
        else:
            avg_var = ""

        device_name = "".join(x for x in self.device_considered if x.isalnum())  # it eliminate characters not valid for filenames
        if not config.timestamp:
            imgPath = '../../output/' + str(device_name) + '_' + str(self.priority_considered) + '_' + 'MC' + locat + info + avg_var
        else:
            imgPath = '../../output/' + str(device_name) + '_' + config.WINDOW + '_' + str(config.CORRELATION_MINUTES) + 'min_' + 'MC'
        mc_graph.render(imgPath)
        os.remove(imgPath)  # remove the source code generated by graphviz

    def assign_color(self, device_location):
        '''
        Returns a dictionary with the location as key and the assigned colour as value (WORKS WITH MAX 10 DIFFERENT LOCATIONS)
        '''
        system_color = ['Blue', 'Green', 'Red', 'Purple', 'Yellow', 'Red', 'Grey', 'Light Red', 'Light Blue', 'Light Green']
        location_color = dict() # key = location; value = color
        for dev, loc in device_location.items():
            if loc not in location_color:
                color = system_color[0]
                system_color.remove(color)
                location_color[loc] = color
        return location_color

    def find_avg_var(self, sourceDev, destDev, hideNames, reference_dev):
        if reference_dev:
            for item in self.ref_dev_avg_vars:
                avg, var = self.__find_avg_var(item, sourceDev, destDev, hideNames)
                if avg != None and var != None:
                    return avg, var
        else:
            for item in self.avg_var_list:
                avg, var = self.__find_avg_var(item, sourceDev, destDev, hideNames)
                if avg != None and var != None:
                    return avg, var
        return None, None

    def getSequencesOfPriority(self, sequencesInTuples):
        cleanedSeq = [] #sequences without priority indication and without tuples surrounding them
        for tup in sequencesInTuples:
            if tup[1] == self.priority_considered:
                cleanedSeq.append(tup[0])
        return cleanedSeq

    def convertNames(self, oldNames, newNamePrefix = "Node"):
        '''Converts the names in the list "oldNames" in strings like "newNamePrefix" + a progressive index.'''
        i = 0
        fakeRealDict = dict()
        for d in oldNames:
            fakeRealDict[d] = newNamePrefix + str(i)
            i += 1
        fakeRealDict[self.device_considered_realName] = "ReferenceDevice"
        return fakeRealDict

    def convertNamesCaesarCipher(self, oldNames, shift):
        ''' Converts the names in the list "oldNames" using a Caesar's Cipher using the given shift value. '''

        fakeRealDict = dict()
        for d in oldNames:
            dName, eName = string.split(d, "--")
            #fakeRealDict[d] = self.shift(dName, shift) + "--" + self.shift(eName, shift)
            fakeRealDict[d] = self.shift(dName, shift) + "--" + eName
        fakeRealDict[self.device_considered_realName] = self.shift(self.device_considered_realName, shift)
        return fakeRealDict

    def shift(self, name, shift):
        ''' Shifts each character of the given name by the given "shift" value '''
        alphabetLower = 'abcdefghijklmnopqrstuvwxyz'
        alphabetUpper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        newName = []
        for i in name:  # iterate over the text
            if i.strip():  # if the char is not a space ""
                if i.isupper():
                    newName.append(alphabetUpper[(alphabetUpper.index(i) + shift) % 26])
                elif i.islower():
                    newName.append(alphabetLower[(alphabetLower.index(i) + shift) % 26])
                elif i.isdigit():
                    newName.append(str((int(i) + shift) % 10))
                else:
                    newName.append("_")
            else:
                newName.append(i)  # if space then simply append it to data
        output = ''.join(newName)
        return output

    def getOnlyDevConsidered(self, sequences):
        finalSeq = []
        for seq in sequences:
            seq = [dev for dev in seq if dev in self.variables_names]
            if seq:
                finalSeq.append(seq)
        return finalSeq

    def __find_avg_var(self, item, sourceDev, destDev, hideNames):
        if hideNames:
            if not "--" in item[0]:
                src = self.shift(item[0], 5)
            else:
                dName, eName = string.split(item[0], "--")
                src = self.shift(dName, 5) + "--" + eName
            dName, eName = string.split(item[1], "--")
            dst = self.shift(dName, 5) + "--" + eName
        else:
            src = item[0]
            dst = item[1]
        if (sourceDev == src and destDev == dst):
            avg = item[2]
            var = item[3]
            avg = avg / 1000
            var = var / 1000
            avg = round(avg, 1)
            var = round(var, 1)
            return avg, var
        return None, None









