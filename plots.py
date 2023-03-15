"""
This file creates all the necessary plots that we use in the final report.
It runs as a main and requires the measurements results to be written in 
the 'results' folder, in the form specified by the {CRUD}.py files.
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
from os.path import join as pjoin
import matplotlib.patches as mpatches


# This file's absolute path
path = os.path.dirname(os.path.realpath(__file__))

# DBMS names
DBMSs = ['neo4j', 'janus']
DBMS_names = ['Neo4j', 'JanusGraph']
# Data scales
scales = [i+1 for i in range(6)]

# Horizontal axis for plots
x = [10**scale for scale in scales]


def generic_plot(op, title, savename):
    plt.figure()
    for dbms in DBMSs:
        min_ = []
        max_ = []
        mean_ = []
        for scale in scales:
            # Read results file
            datafile = pjoin(path, 'results', f'{dbms}_{op}_scale{scale}.csv')
            try:
                data = pd.read_csv(datafile)
                # Isolate metrics
                min_.append(data.iloc[0, 1:].min())
                max_.append(data.iloc[1, 1:].max())
                mean_.append(data.iloc[2, 1:].mean())
            except FileNotFoundError:
                pass
        # Plot
        colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
        plt.plot(x, mean_, colors[0], linewidth=2)
        plt.fill_between(x, min_, max_, color=colors[0], alpha=0.1)
    plt.xscale('log')
    plt.title(title)
    plt.xlabel('Dataset node scale')
    plt.ylabel('Time (sec)')
    blue_patch = mpatches.Patch(color='blue', label=DBMS_names[0])
    red_patch = mpatches.Patch(color='red', label=DBMS_names[1])
    plt.legend(handles=[blue_patch, red_patch])
    plt.grid()
    plt.savefig(pjoin(path, 'plots', savename))


# fill_and_empty plot =========================================================
plt.figure()
for i, dbms in enumerate(DBMSs):
    min_fill = []
    max_fill= []
    mean_fill = []
    min_empty = []
    max_empty = []
    mean_empty = []
    for scale in scales:
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_fill_empty_scale{scale}.csv')
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
    plt.plot(x, mean_fill, colors[0], label=f'{DBMS_names[i]}, Fill', linewidth=2)
    plt.fill_between(x, min_fill, max_fill, color=colors[0], alpha=0.1)
    plt.plot(x, mean_empty, colors[1] + '--', label=f'{DBMS_names[i]}, Empty', linewidth=2)
    plt.fill_between(x, min_empty, max_empty, color=colors[1], linestyle='--', alpha=0.1)
plt.xscale('log')
plt.yscale('log')
plt.title('Duration of filling and emptying a database')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'fill_and_empty.pdf'))


# CREATE plot =================================================================
plt.figure()
for i, dbms in enumerate(DBMSs):
    min_node = []
    max_node = []
    mean_node = []
    min_edge = []
    max_edge = []
    mean_edge = []
    for scale in scales:
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_create_scale{scale}.csv')
        try:
            data = pd.read_csv(datafile)
            # Isolate metrics
            min_node.append(data.loc[0, ['create_paper', 'create_author']].min())
            max_node.append(data.loc[1, ['create_paper', 'create_author']].max())
            mean_node.append(data.loc[2, ['create_paper', 'create_author']].mean())
            min_edge.append(data.loc[0, ['create_reference', 'create_authorship']].min())
            max_edge.append(data.loc[1, ['create_reference', 'create_authorship']].max())
            mean_edge.append(data.loc[2, ['create_reference', 'create_authorship']].mean())
        except FileNotFoundError:
            pass
    # Plot
    colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
    plt.plot(x, mean_node, colors[0], label=f'{DBMS_names[i]}, Nodes', linewidth=2)
    plt.fill_between(x, min_node, max_node, color=colors[0], alpha=0.1)
    plt.plot(x, mean_edge, colors[1] + '--', label=f'{DBMS_names[i]}, Edges', linewidth=2)
    plt.fill_between(x, min_edge, max_edge, color=colors[1], linestyle='--', alpha=0.1)
plt.xscale('log')
plt.title('Transaction duration of CREATE')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'create.pdf'))


# READ plots ==================================================================
plt.figure()
for i, dbms in enumerate(DBMSs):
    min_simple = []
    max_simple = []
    mean_simple = []
    min_fancier = []
    max_fancier = []
    mean_fancier = []
    for scale in scales:
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_read_scale{scale}.csv')
        try:
            data = pd.read_csv(datafile)
            # Isolate metrics
            min_simple.append(data.loc[0, ['title_of_paper']].min())
            max_simple.append(data.loc[1, ['title_of_paper']].max())
            mean_simple.append(data.loc[2, ['title_of_paper']].mean())
        except FileNotFoundError:
            pass
    # Plot
    colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
    plt.plot(x, mean_simple, colors[0], label=f'{DBMS_names[i]}', linewidth=2)
    plt.fill_between(x, min_simple, max_simple, color=colors[0], alpha=0.1)
plt.xscale('log')
plt.title('Transaction duration of READ (simple query)')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'read_simple.pdf'))

plt.figure()
for i, dbms in enumerate(DBMSs):
    min_simple = []
    max_simple = []
    mean_simple = []
    min_fancier = []
    max_fancier = []
    mean_fancier = []
    for scale in scales:
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_read_scale{scale}.csv')
        try:
            data = pd.read_csv(datafile)
            # Isolate metrics
            min_simple.append(data.loc[0, ['authors_of']].min())
            max_simple.append(data.loc[1, ['authors_of']].max())
            mean_simple.append(data.loc[2, ['authors_of']].mean())
        except FileNotFoundError:
            pass
    # Plot
    colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
    plt.plot(x, mean_simple, colors[0], label=f'{DBMS_names[i]}', linewidth=2)
    plt.fill_between(x, min_simple, max_simple, color=colors[0], alpha=0.1)
plt.xscale('log')
plt.title('Transaction duration of READ (adjacency query)')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'read_adjacency.pdf'))

plt.figure()
for i, dbms in enumerate(DBMSs):
    min_simple = []
    max_simple = []
    mean_simple = []
    min_fancier = []
    max_fancier = []
    mean_fancier = []
    for scale in scales:
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_read_scale{scale}.csv')
        try:
            data = pd.read_csv(datafile)
            # Isolate metrics
            min_simple.append(data.loc[0, ['are_collaborators']].min())
            max_simple.append(data.loc[1, ['are_collaborators']].max())
            mean_simple.append(data.loc[2, ['are_collaborators']].mean())
        except FileNotFoundError:
            pass
    # Plot
    colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
    plt.plot(x, mean_simple, colors[0], label=f'{DBMS_names[i]}', linewidth=2)
    plt.fill_between(x, min_simple, max_simple, color=colors[0], alpha=0.1)
plt.xscale('log')
plt.title('Transaction duration of READ (reachability query)')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'read_reachability.pdf'))

plt.figure()
for i, dbms in enumerate(DBMSs):
    if dbms == 'janus':
        continue
    min_simple = []
    max_simple = []
    mean_simple = []
    min_fancier = []
    max_fancier = []
    mean_fancier = []
    for scale in scales:
        # Read results file
        datafile = pjoin(path, 'results', f'{dbms}_read_scale{scale}.csv')
        try:
            data = pd.read_csv(datafile)
            # Isolate metrics
            min_simple.append(data.loc[0, ['mean_authors_per_paper']].min())
            max_simple.append(data.loc[1, ['mean_authors_per_paper']].max())
            mean_simple.append(data.loc[2, ['mean_authors_per_paper']].mean())
        except FileNotFoundError:
            pass
    # Plot
    colors = ('b', 'g') if dbms == 'neo4j' else ('r', 'm')
    plt.plot(x, mean_simple, colors[0], label=f'{DBMS_names[i]}', linewidth=2)
    plt.fill_between(x, min_simple, max_simple, color=colors[0], alpha=0.1)
plt.xscale('log')
plt.title('Transaction duration of READ (aggregate query)')
plt.xlabel('Dataset node scale')
plt.ylabel('Time (sec)')
plt.legend()
plt.grid()
plt.savefig(pjoin(path, 'plots', 'read_aggregate.pdf'))


# UPDATE plot =================================================================
generic_plot('update', 'Transaction duration of UPDATE', 'update.pdf')


# DELETE plot =================================================================
generic_plot('delete', 'Transaction duration of DELETE', 'delete.pdf')
