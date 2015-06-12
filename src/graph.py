#!/usr/bin/python

GRAPH_LIB_NOT_FOUND = """
        if you need to visualize the flow, you need
            networkx http://networkx.lanl.gov/
            graphviz http://www.graphviz.org/
                     http://www.graphviz.org/content/attrs

            pip install networkx
            # pip install pygraphviz
            # https://github.com/pygraphviz/pygraphviz/issues/40
        otherwise, disable the graph method, set_output_graph_file(), of JobBlock
"""

try:
    import networkx as nx
    #import matplotlib.pyplot as plt # not used
    #import pygraphviz as pgv # not used
except ImportError:
    nx = None

from datetime import datetime

class NetGraph(object):
    def __init__(self, filepath=None):
        if nx is None:
            self.is_graph_lib_available = False
        else:
            self.is_graph_lib_available = True

            self.G = nx.MultiDiGraph()

            # alternative method ##
            #self.G = pgv.AGraph()

            self.edges = {}
            self.node_info = {}
            self.filepath = filepath

    def _load(self):
        nodes = []
        for e, meta in self.edges.items():
            u, v = e.split("\t")
            self.G.add_edge(u, v, label=meta['msg'], fontname='Arial Black',
                            color=meta['edge_color'], penwidth=meta['weight'],
                            )
            if u not in nodes:
                self.G.add_node(u, fontname='Arial Black', color=meta['from_color'])
                nodes.append(u)
            if v not in nodes:
                self.G.add_node(v, fontname='Arial Black', color=meta['to_color'])
                nodes.append(v)
    def _draw(self):
        pos=nx.graphviz_layout(self.G, prog='dot')
        nx.draw(self.G, pos,node_size=100, alpha=0.4, edge_color='r', font_size=16)
    def add_edge(self, u, v, meta):
        k = u+'\t'+v
        self.edges[k] = meta
    def add_edges(self, uvms):
        for (u, v, m) in uvms:
            self.add_edge(u, v, m)
        print self.edges

    def show(self,node_size_weight=None, edge_size_weight=None, file_name=None):
        self._load()
        self._draw()
        #plt.show()

        if self.filepath is not None:
            nx.write_dot(self.G, self.filepath)

        # alternative method
        #self.G.draw('star.png',prog="dot")
        #self.G.write('grid.dot')
        return

if __name__ == '__main__':
    '''
    G=nx.MultiDiGraph() # create empty graph
    G.add_edge('a','b', None, message='23321')
    pos=nx.spring_layout(G,iterations=10)
    nx.draw(G,pos,node_size=0,alpha=0.4,edge_color='r',font_size=16)
    plt.show()
    exit(0)
    '''
    g = NetGraph()
    g.add_edges(
        [
            ['a', 'b', 'a>b'],
            ['b', 'c', 'b>c'],
            ['c', 'a', 'c>a'],
        ]
    )
    g.show()
