import os
import pandas as pd
import matplotlib.pyplot as plt
from os.path import join as pjoin


# This file's absolute path
path = os.path.dirname(os.path.realpath(__file__))

# DBMS names
DBMSs = ['neo4j']#, 'janus']
# Database names, each one contains data on a different scale 
scales = [i+1 for i in range(4)]

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
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_create_scale{scale}.csv')
        data = pd.read_csv(datafile)
        # Isolate metrics
        min_node.append(data.loc[0, ['create_paper', 'create_author']].min())
        max_node.append(data.loc[1, ['create_paper', 'create_author']].max())
        mean_node.append(data.loc[2, ['create_paper', 'create_author']].mean())
        min_edge.append(data.loc[0, ['create_reference', 'create_authorship']].min())
        max_edge.append(data.loc[1, ['create_reference', 'create_authorship']].max())
        mean_edge.append(data.loc[2, ['create_reference', 'create_authorship']].mean())

    # Plot
    plt.figure()
    plt.plot(x, mean_node, 'b', label='Nodes')
    plt.fill_between(x, min_node, max_node, color='b', alpha=0.25)
    plt.plot(x, mean_edge, 'g--', label='Edges')
    plt.fill_between(x, min_edge, max_edge, color='g', linestyle='--', alpha=0.25)
    plt.xscale('log')
    plt.title('Transaction duration of CREATE')
    plt.xlabel('Dataset node scale')
    plt.ylabel('Time (sec)')
    plt.legend()
    plt.grid()
    plt.savefig(pjoin(path, 'plots', 'create.pdf'))
