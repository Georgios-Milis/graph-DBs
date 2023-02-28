"""
This file creates all the necessary plots that we use in the final report.
It runs as a main and requires the measurements results to be written in 
the 'results' folder, in the form specified by the {CRUD}_run.py files.
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
from os.path import join as pjoin


# This file's absolute path
path = os.path.dirname(os.path.realpath(__file__))

# DBMS names
DBMSs = ['neo4j']#, 'janus']
# Data scales
scales = [i+1 for i in range(3)]

# Horizontal axis for plots
x = [10**scale for scale in scales]


# fill_and_empty plot =========================================================
plt.figure()
for dbms in DBMSs:
    min_fill = []
    max_fill= []
    mean_fill = []
    min_empty = []
    max_empty = []
    mean_empty = []
    for scale in scales:
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_fill_and_empty_scale{scale}.csv')
        data = pd.read_csv(datafile)
        # Isolate metrics
        min_fill.append(data.loc[0, 'fill_database'])
        max_fill.append(data.loc[1, 'fill_database'])
        mean_fill.append(data.loc[2, 'fill_database'])
        min_empty.append(data.loc[0, 'clear_database'])
        max_empty.append(data.loc[1, 'clear_database'])
        mean_empty.append(data.loc[2, 'clear_database'])

    # Plot
    colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
    plt.plot(x, mean_fill, colors[0], label='Fill', linewidth=2)
    plt.fill_between(x, min_fill, max_fill, color=colors[0], alpha=0.2)
    plt.plot(x, mean_empty, colors[1] + '--', label='Empty', linewidth=2)
    plt.fill_between(x, min_empty, max_empty, color=colors[1], linestyle='--', alpha=0.2)
plt.xscale('log')
plt.title('Duration of filling and emptying a database')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'fill_and_empty.pdf'))


# CREATE plot =================================================================
plt.figure()
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
    colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
    plt.plot(x, mean_node, colors[0], label='Nodes', linewidth=2)
    plt.fill_between(x, min_node, max_node, color=colors[0], alpha=0.2)
    plt.plot(x, mean_edge, colors[1] + '--', label='Edges', linewidth=2)
    plt.fill_between(x, min_edge, max_edge, color=colors[1], linestyle='--', alpha=0.2)
plt.xscale('log')
plt.title('Transaction duration of CREATE')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'create.pdf'))


# READ plot ===================================================================
# plt.figure()
# for dbms in DBMSs:
#     min_node = []
#     max_node = []
#     mean_node = []
#     min_edge = []
#     max_edge = []
#     mean_edge = []
#     for scale in scales:
#         # Read results file
#         datafile = pjoin(path, 'results', f'{dbms}_create_scale{scale}.csv')
#         data = pd.read_csv(datafile)
#         # Isolate metrics
#         min_node.append(data.loc[0, ['create_paper', 'create_author']].min())
#         max_node.append(data.loc[1, ['create_paper', 'create_author']].max())
#         mean_node.append(data.loc[2, ['create_paper', 'create_author']].mean())
#         min_edge.append(data.loc[0, ['create_reference', 'create_authorship']].min())
#         max_edge.append(data.loc[1, ['create_reference', 'create_authorship']].max())
#         mean_edge.append(data.loc[2, ['create_reference', 'create_authorship']].mean())

#     # Plot
#     colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
#     plt.plot(x, mean_node, colors[0], label='Nodes', linewidth=2)
#     plt.fill_between(x, min_node, max_node, color=colors[0], alpha=0.2)
#     plt.plot(x, mean_edge, colors[1] + '--', label='Edges', linewidth=2)
#     plt.fill_between(x, min_edge, max_edge, color=colors[1], linestyle='--', alpha=0.2)
# plt.xscale('log')
# plt.title('Transaction duration of READ')
# plt.xlabel('Dataset node scale')
# plt.ylabel('Time (sec)')
# plt.legend()
# plt.grid()
# plt.savefig(pjoin(path, 'plots', 'create.pdf'))