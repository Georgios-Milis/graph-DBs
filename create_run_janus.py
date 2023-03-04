from gremlin_python.driver import client
from tornado import httpclient
import create_queries_janus
import data
import json
import os
import time
import numpy as np
import pandas as pd


ws_url = 'ws://localhost:8182/gremlin'
ws_conn = httpclient.HTTPRequest(ws_url)
gremlin_conn = client.Client(ws_conn, "g")


query = "graph = TinkerGraph.open()"
gremlin_conn.submit(query)
query1 = "g = graph.traversal()"
gremlin_conn.submit(query1)



paper_create_data = [ [i,'Title' + f'{i}' , 1234,0] for i in range(1,11) ]
author_create_data = [ [i,'name' + f'{i}' , 'NTUA'] for i in range(11,21) ]
reference_create_data = [ [i , i + 100] for i in range(1,11) ]
authorship_create_data = [ [i+10,i] for i in range(1,11) ]


paper_create_times = np.empty(10)
author_create_times = np.empty(10)
reference_create_times = np.empty(10)
authorship_create_times = np.empty(10)

result = np.empty([3,4])

for i in range(10):
    time_start = time.time()
    create_queries_janus.create_paper(gremlin_conn,paper_create_data[i][0],paper_create_data[i][1],paper_create_data[i][2],paper_create_data[i][3])
    time_end = time.time()
    paper_create_times[i] = time_end-time_start

result[0][0] = np.min(paper_create_times) 
result[1][0] = np.max(paper_create_times) 
result[2][0] = np.mean(paper_create_times)


for i in range(10):
    time_start = time.time()
    create_queries_janus.create_author(gremlin_conn,author_create_data[i][0],author_create_data[i][1],author_create_data[i][2])
    time_end = time.time()
    author_create_times[i]= time_end-time_start


result[0][1] = np.min(author_create_times) 
result[1][1] = np.max(author_create_times) 
result[2][1] = np.mean(author_create_times)


paper_create_data1 = [ [i + 100,'Title' + f'{i}' , 1234,0] for i in range(1,11) ]

for i in range(10):
    create_queries_janus.create_paper(gremlin_conn,paper_create_data1[i][0],paper_create_data1[i][1],paper_create_data1[i][2],paper_create_data1[i][3])
    time_start = time.time()
    create_queries_janus.create_reference(gremlin_conn,reference_create_data[i][0],reference_create_data[i][1])
    time_end = time.time()
    reference_create_times[i]= time_end-time_start


result[0][2] = np.min(reference_create_times) 
result[1][2] = np.max(reference_create_times) 
result[2][2] = np.mean(reference_create_times)

for i in range(10):
    time_start = time.time()
    create_queries_janus.create_authorship(gremlin_conn,authorship_create_data[i][0],authorship_create_data[i][1])
    time_end = time.time()
    authorship_create_times[i]= time_end-time_start

result[0][3] = np.min(authorship_create_times) 
result[1][3] = np.max(authorship_create_times) 
result[2][3] = np.mean(authorship_create_times)


df = pd.DataFrame(result, columns = ['create_paper', 'create_author','create_reference','create_authorship'] , index=['min', 'max', 'mean'])
print(df)
print(result)
