import numpy as np
import mysql.connector  # pip install mysql-connector-python
import os
import config
import re
import pomegranate as pomgr
import graphviz as gv
import expandDeviceMarkov
from DataError import DataError

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

    def draw_mc_model(self, location_choice, info_choice, avg_var_edges, refDevice, hideNames):
        if location_choice and info_choice:
            DataError("location_choice and info_choice can't be both True")
            return
        mc_graph = gv.Digraph(format="png")
        mc_graph.graph_attr['overlap'] = "false"
        mc_graph.graph_attr['rankdir'] = 'LR'
        devices = self.variables_names

        # Automatically change the names of the nodes
        if hideNames:
            # realFakeNamesDict = self.convertNamesInNodes(devices)
            realFakeNamesDict = self.convertNamesCaesarCipher(devices, 5)

        # Create subgraph for the locations
        if location_choice:
            device_location = dict()
            device_locationH1 = dict()

            # For H0 and H1:
            for d in devices:
                allDevicesLocations = self.general_handler.get_device_locations()
                device_location[d] = allDevicesLocations[d][0]  # H0
                device_locationH1[d] = allDevicesLocations[d][1]  # H1
            if refDevice:
                ref = self.device_considered_realName
                device_location[ref] = allDevicesLocations[ref][0] #H0
                device_locationH1[ref] = allDevicesLocations[ref][1] #H1
            location_color = self.assign_color(device_location)
            location_colorH1 = self.assign_color(device_locationH1)

            # Creating the subgraphs, one for each location:
            loc_subgraphs = dict()
            for loc in location_color:
                name = "cluster_" + loc  # SE IL NOME DEL SOTTOGRAFO NON INIZIA PER "cluster_" NON FUNZIONERA'
                loc_subgraphs[loc] = gv.Digraph(name)
                loc_subgraphs[loc].graph_attr['label'] = loc  # Label with name to be visualized in the image
                loc_subgraphs[loc].graph_attr['overlap'] = "false"

        # Create subgraphs for occurrences, average and standard deviation:
        if info_choice:
            info_subgraphs = dict()
            # there's one subgraph for each node:
            id = 0
            for d in devices:
                name = "cluster_" + str(id)  # SE IL NOME DEL SOTTOGRAFO NON INIZIA PER "cluster_" NON FUNZIONERA'
                id += 1
                if hideNames:
                    devName = realFakeNamesDict[d]
                else:
                    devName = d
                info_subgraphs[devName] = gv.Digraph(name)
                label = "Occurrences: " + str(round(self.occurrences[d], 2)) #+ " | "
                #label = label + "Avg: " + str(round(self.devicesColumnDict[d].msAverage / 1000, 2)) + "s\n"
                #label = label + "St.Dev.: " + str(round(self.devicesColumnDict[d].msStandDev / 1000, 2)) + "s"
                info_subgraphs[devName].graph_attr['label'] = label  # Label with name to be visualized in the image
                info_subgraphs[devName].graph_attr['overlap'] = 'scale'

        # Create nodes
        for node in devices:
            if location_choice:
                locationH0 = device_location[node]
                locationH1 = device_locationH1[node]
                loc_subgraphs[locationH0].node(node, style='filled', fillcolor=location_colorH1[locationH1])# add the node to the right subgraph
                # loc_subgraphs[locationH0].node(node) #USE THIS TO ADD ONLY H0
            elif info_choice:
                if hideNames:
                    devName = realFakeNamesDict[node]
                else:
                    devName = node
                info_subgraphs[devName].node(devName)
            else:  # add the node directly to the graph
                if hideNames:
                    node = realFakeNamesDict[node]
                mc_graph.node(node)

        # Reference device
        if refDevice and location_choice:
            ref = self.device_considered_realName
            locationH0 = device_location[ref]
            locationH1 = device_locationH1[ref]
            loc_subgraphs[locationH0].node(ref, style='filled', fillcolor=location_colorH1[locationH1])
        else:
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
        if location_choice:
            legend = ""
            for loc in location_colorH1:
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
            if (prob < 0.1 and prob >= 0.0):
                #mc_graph.edge(edge0, edge1, color="Grey")
                pass
            else:
                if avg_var_edges:
                    avg, var = self.find_avg_var(edge0, edge1, hideNames, False)
                    if(avg == None or var == None):
                        lab = str(prob)
                    else:
                        lab = str(prob) + " | (" + str(avg) + "," + str(var) + ")"
                else:
                    lab = str(prob)

                if (edge1, edge0) in drawnEdges:
                    #oldLink rewriting
                    oldLink = (edge1, edge0)
                    oldLab = drawnEdges[oldLink]
                    if (prob >= 0.1 and prob < 0.5):
                        mc_graph.edge(edge1, edge0, color="black", label=oldLab, portPos="nw", style="invis")
                    elif (prob >= 0.5):
                        mc_graph.edge(edge1, edge0, color="red", label=oldLab, portPos="nw", style="invis")
                    #new link in opposite direction
                    if (prob >= 0.1 and prob < 0.5):
                        mc_graph.edge(edge0, edge1, color="black", label=lab, portPos="se")
                    elif (prob >= 0.5):
                        mc_graph.edge(edge0, edge1, color="red", label=lab, portPos="se")
                else:
                    if (prob >= 0.1 and prob < 0.5):
                        mc_graph.edge(edge0, edge1, color="black", label=lab)
                    elif (prob >= 0.5):
                        mc_graph.edge(edge0, edge1, color="red", label=lab)
                #it is in drawnEdges only if prob > 0.1
                drawnEdges[(edge0, edge1)] = lab

        if refDevice:
            devProbList = self.mc.distributions[0].parameters
            for i in range(0, len(devProbList)):
                for node in devProbList[i]:
                    prob = round(devProbList[i][node], 2)
                    if prob > 0.1:
                        if avg_var_edges:
                            avg, var = self.find_avg_var(self.device_considered_realName, node, hideNames, True)
                            if (avg == None or var == None):
                                lab = str(prob)
                            else:
                                lab = str(prob) + " | (" + str(avg) + "," + str(var) + ")"
                        else:
                            lab = str(prob)
                        if hideNames:
                            #mc_graph.edge(realFakeNamesDict[self.device_considered_realName], realFakeNamesDict[node],
                            #              color="blue", taillabel=lab, labeldistance = "2.5")
                            mc_graph.edge(realFakeNamesDict[self.device_considered_realName], realFakeNamesDict[node],
                                          color="blue", label=lab)
                        else:
                            mc_graph.edge(self.device_considered_realName, node, color="blue", label=lab)

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
        imgPath = '../../output/' + str(device_name) + '_' + str(self.priority_considered) + '_' + 'MC' + locat + info + avg_var
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
                if hideNames:
                    source = self.shift(sourceDev, 5)
                    destination = self.shift(destDev, 5)
                avg, var = self.__find_avg_var(item, source, destination, hideNames)
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

    def getOnlyDevConsidered(self, sequences):
        finalSeq = []
        for seq in sequences:
            seq = [dev for dev in seq if dev in self.variables_names]
            if seq:
                finalSeq.append(seq)
        return finalSeq

    def convertNamesCaesarCipher(self, devices, shift):
        fakeRealDict = dict()
        for d in devices:
            fakeRealDict[d] = self.shift(d, shift)
        fakeRealDict[self.device_considered_realName] = self.shift(self.device_considered_realName, shift)
        return fakeRealDict

    def shift(self, name, shift):
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
                newName.append(i)  # if space the simply append it to data
        output = ''.join(newName)
        return output

    def __find_avg_var(self, item, sourceDev, destDev, hideNames):
        if hideNames:
            src = self.shift(item[0], 5)
            dst = self.shift(item[1], 5)
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









