import os
import re
import numpy as np
import pandas as pd
from time import time
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import JanusGraphConnection, transact_and_time


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Config
load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')


# Dataset files
datafiles = sorted([
    pjoin(path, 'data', f) for f in os.listdir(pjoin(path, 'data'))
    if re.search("^scale[1-2].*\.txt", f)
])


for scale, datafile in enumerate(datafiles, 1):
    # Initialize connection to database
    connection = JanusGraphConnection(URI)
    papers = data.get_papers_data(datafile)
    authors = data.get_authors_data(datafile)
    citations = data.get_citations_data(datafile)
    authorships = data.get_authorships_data(datafile)

    paper_read_times = np.empty(10)
    author_read_times = np.empty(10)
    reference_read_times = np.empty(10)
    authorship_read_times = np.empty(10)

    result = np.empty([3,4])

    for i in range(10):
        paperid = str(papers[i]['id'])
        time_start = time()
        connection.find_paper(paperid)
        time_end = time()
        paper_read_times[i] = time_end-time_start

    result[0][0] = np.min(paper_read_times) 
    result[1][0] = np.max(paper_read_times) 
    result[2][0] = np.mean(paper_read_times)


    for i in range(10):
        paperid = str(papers[i]['id'])
        time_start = time()
        connection.authors_of(papers[i]['id'])
        time_end = time()
        author_read_times[i]= time_end-time_start


    result[0][1] = np.min(author_read_times) 
    result[1][1] = np.max(author_read_times) 
    result[2][1] = np.mean(author_read_times)



    for i in range(10):
        authorid1 = str(authors[i]['id'])
        authorid2 = str(authors[i+1]['id'])
        time_start = time()
        connection.are_collaborators( authorid1, authorid2 )
        time_end = time()
        reference_read_times[i]= time_end-time_start


    result[0][2] = np.min(reference_read_times) 
    result[1][2] = np.max(reference_read_times) 
    result[2][2] = np.mean(reference_read_times)



    # TODO milis: save

    df = pd.DataFrame(result, columns = ['create_paper', 'create_author','create_reference','create_authorship'] , index=['min', 'max', 'mean'])
    print(df)
    print(result)