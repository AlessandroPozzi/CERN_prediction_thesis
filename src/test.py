'''
Created on 21 dic 2017

@author: Alessandro Corsair
'''
import pydot

nice_graph = pydot.Dot(graph_type='digraph')

#NODES
nice_graph.add_node(pydot.Node("A"))
nice_graph.add_node(pydot.Node("B"))
nice_graph.add_node(pydot.Node("C"))
nice_graph.add_node(pydot.Node("D"))
nice_graph.add_node(pydot.Node("E"))

#EDGES
nice_graph.add_edge(pydot.Edge("A", "B"))
nice_graph.add_edge(pydot.Edge("A", "C"))
nice_graph.add_edge(pydot.Edge("C", "D"))
nice_graph.add_edge(pydot.Edge("B", "D"))
                    
nice_graph.write_png('../output/example_network.png')