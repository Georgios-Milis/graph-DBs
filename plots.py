import os
import pandas as pd
import matplotlib.pyplot as plt
from os.path import join as pjoin


# This file's absolute path
path = os.path.dirname(os.path.realpath(__file__))

# DBMS names
DBMSs = ['neo4j']#, 'janus']
# Database names, each one contains data on a different scale 
scales = [i+1 for i in range(6)]

# Horizontal axis for plots
x = [10**scale for scale in scales]

for dbms in DBMSs:
    min_node = []
    max_node = []
    mean_node = []
    min_edge = []
    max_edge = []
    mean_edge = []
    for scale in scales:
        datafile = pjoin(path, 'results', f'{dbms}_create_scale{scale}.csv')
        data = pd.read_csv(datafile)

        min_node.append(data.min([0, ['create_paper', 'create_author']]))
        max_node.append(data.max([1, ['create_paper', 'create_author']]))
        mean_node.append(data.mean([2, ['create_paper', 'create_author']]))
        min_edge.append(data.min([0, ['create_reference', 'create_authorship']]))
        max_edge.append(data.max([0, ['create_reference', 'create_authorship']]))
        mean_edge.append(data.mean([0, ['create_reference', 'create_authorship']]))

    print(min_edge)

    plt.figure()
    plt.plot(x, mean_node, 'b', label='Nodes')
    plt.fill_between(x, min_node, max_node, 'b', alpha=0.25)
    plt.plot(x, mean_edge, 'g', Label='Edges')
    plt.fill_between(x, min_edge, max_edge, 'g', alpha=0.25)
    plt.title('Transaction duration of CREATE')
    plt.xscale('log')
    plt.xlabel('Dataset node scale')
    plt.ylabel('Time (sec)')
    plt.grid()
    plt.legend()
    plt.savefig(pjoin(path, 'plots', 'create.py'))
