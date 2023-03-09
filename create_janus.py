import os
import re
import numpy as np
import pandas as pd
from time import time
from dotenv import load_dotenv
from os.path import join as pjoin

from connection import JanusGraphConnection, transact_and_time


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Config
load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')


# Dataset files
datafiles = sorted([
    pjoin(path, '..', 'data', f) for f in os.listdir(pjoin(path, 'data'))
    if re.search("^scale[1-2].*\.txt", f)
])

for scale, datafile in enumerate(datafiles, 1):
    # Initialize connection to database
    connection = JanusGraphConnection(URI)

    paper_create_data = [[i, 'Title' + f'{i}' , 1234, 0] for i in range(1, 11)]
    author_create_data = [[i, 'name' + f'{i}' , 'NTUA'] for i in range(11, 21)]
    reference_create_data = [[i, i + 100] for i in range(1, 11)]
    authorship_create_data = [[i + 10, i] for i in range(1, 11)]

    paper_create_times = np.empty(10)
    author_create_times = np.empty(10)
    reference_create_times = np.empty(10)
    authorship_create_times = np.empty(10)

    result = np.empty([3,4])

    for i in range(10):
        time_start = time()
        connection.create_paper(paper_create_data[i][0], paper_create_data[i][1], paper_create_data[i][2], paper_create_data[i][3])
        time_end = time()
        paper_create_times[i] = time_end - time_start

    result[0][0] = np.min(paper_create_times) 
    result[1][0] = np.max(paper_create_times) 
    result[2][0] = np.mean(paper_create_times)


    for i in range(10):
        time_start = time()
        connection.create_author(author_create_data[i][0], author_create_data[i][1], author_create_data[i][2])
        time_end = time()
        author_create_times[i] = time_end - time_start


    result[0][1] = np.min(author_create_times) 
    result[1][1] = np.max(author_create_times) 
    result[2][1] = np.mean(author_create_times)


    paper_create_data1 = [[i + 100, 'Title' + f'{i}', 1234, 0] for i in range(1, 11)]

    for i in range(10):
        connection.create_paper(paper_create_data1[i][0], paper_create_data1[i][1], paper_create_data1[i][2], paper_create_data1[i][3])
        time_start = time()
        connection.create_reference(reference_create_data[i][0], reference_create_data[i][1])
        time_end = time()
        reference_create_times[i] = time_end - time_start


    result[0][2] = np.min(reference_create_times) 
    result[1][2] = np.max(reference_create_times) 
    result[2][2] = np.mean(reference_create_times)

    for i in range(10):
        time_start = time()
        connection.create_authorship(authorship_create_data[i][0], authorship_create_data[i][1])
        time_end = time()
        authorship_create_times[i] = time_end - time_start

    result[0][3] = np.min(authorship_create_times) 
    result[1][3] = np.max(authorship_create_times) 
    result[2][3] = np.mean(authorship_create_times)


    # TODO milis: save

    df = pd.DataFrame(result, columns = ['create_paper', 'create_author','create_reference','create_authorship'] , index=['min', 'max', 'mean'])
    print(df)
    print(result)
