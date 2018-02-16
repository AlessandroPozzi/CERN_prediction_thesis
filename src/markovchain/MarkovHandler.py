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

    def create_mc_model(self):
        self.mc = expandDeviceMarkov.create_mc_model(self.device_considered_realName, self.priority_considered, self.variables_names)


    def draw_mc_model(self, location_choice, info_choice):
        if location_choice and info_choice:
            DataError("location_choice and info_choice can't be both True")
            return
        mc_graph = gv.Digraph(format="png")

        devices = self.variables_names

        # Create subgraph for the locations
        if location_choice:
            device_location = dict()
            device_locationH1 = dict()

            # For H0 and H1:
            for d in devices:
                allDevicesLocations = self.general_handler.get_device_locations()
                device_location[d] = allDevicesLocations[d][0]  # H0
                device_locationH1[d] = allDevicesLocations[d][1]  # H1
            location_color = self.assign_color(device_location)
            location_colorH1 = self.assign_color(device_locationH1)

            # Creating the subgraphs, one for each location:
            loc_subgraphs = dict()
            for loc in location_color:
                name = "cluster_" + loc  # SE IL NOME DEL SOTTOGRAFO NON INIZIA PER "cluster_" NON FUNZIONERA'
                loc_subgraphs[loc] = gv.Digraph(name)
                loc_subgraphs[loc].graph_attr['label'] = loc  # Label with name to be visualized in the image

        # Create subgraphs for occurrences, average and standard deviation:
        if info_choice:
            info_subgraphs = dict()
            # there's one subgraph for each node:
            id = 0
            for d in devices:
                name = "cluster_" + str(id)  # SE IL NOME DEL SOTTOGRAFO NON INIZIA PER "cluster_" NON FUNZIONERA'
                id += 1
                info_subgraphs[d] = gv.Digraph(name)
                label = "Occurrences: " + str(round(self.occurrences[d], 2)) #+ " | "
                #label = label + "Avg: " + str(round(self.devicesColumnDict[d].msAverage / 1000, 2)) + "s\n"
                #label = label + "St.Dev.: " + str(round(self.devicesColumnDict[d].msStandDev / 1000, 2)) + "s"
                info_subgraphs[d].graph_attr['label'] = label  # Label with name to be visualized in the image
                # info_subgraphs[d].graph_attr['penwidth'] = 0

        # Create nodes
        for node in devices:
            if location_choice:
                locationH0 = device_location[node]
                locationH1 = device_locationH1[node]
                loc_subgraphs[locationH0].node(node, style='filled', fillcolor=location_colorH1[
                    locationH1])  # add the node to the right subgraph
                # loc_subgraphs[locationH0].node(node) #USE THIS TO ADD ONLY H0
            elif info_choice:
                info_subgraphs[node].node(node)
            else:  # add the node directly to the graph
                mc_graph.node(node)

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
        for i in range(0, len(cpt)):
            prob = round(cpt[i][2], 2)
            if(prob >= 0.5):
                mc_graph.edge(cpt[i][0], cpt[i][1], color="red", label=str(prob))
            elif(prob >= 0.1 and prob < 0.5):
                mc_graph.edge(cpt[i][0], cpt[i][1], color="black", label=str(prob))
            elif(prob < 0.1 and prob > 0.0):
                mc_graph.edge(cpt[i][0], cpt[i][1], color="Grey")


        # save the .png graph
        if location_choice:
            locat = "_H0H1"
        else:
            locat = ""
        if info_choice:
            info = "_Info"
        else:
            info = ""

        device_name = "".join(x for x in self.device_considered if x.isalnum())  # it eliminate characters not valid for filenames
        imgPath = '../../output/' + str(device_name) + '_' + str(self.priority_considered) + '_' + 'MC' + locat + info
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



