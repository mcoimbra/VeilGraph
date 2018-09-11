import networkx
import sys

gml_graph = networkx.read_gml(sys.argv[1], label='id')

with open(sys.argv[2], 'w') as vertices:
    print('# ', end='', file=vertices)
    print('id', *gml_graph.nodes(data=True)[0][1].keys(), sep=";", file=vertices)

    for node, data in gml_graph.nodes_iter(data=True):
        print(node, *data.values(), sep=';', file=vertices)

with open(sys.argv[3], 'w') as edges:
    print('# source;target', file=edges)
    for s, t, data in gml_graph.edges_iter(data=True):
        print(s, t, *data.values(), sep=';', file=edges)
