from gremlin_python.driver import client
from tornado import httpclient
from queries import read_queries_janus
from queries import create_queries
from queries import delete_queries_janus
from queries import update_queries_janus
import data
import json
import os
import time

ws_url = 'ws://localhost:8182/gremlin'
ws_conn = httpclient.HTTPRequest(ws_url)
gremlin_conn = client.Client(ws_conn, "g")


query = "graph = TinkerGraph.open()"
gremlin_conn.submit(query)
query1 = "g = graph.traversal()"
gremlin_conn.submit(query1)


datafile = './data/scale3_Spambot.txt'
papers = data.get_papers_data(datafile)
authors = data.get_authors_data(datafile)
refs = data.get_citations_data(datafile)
auths = data.get_authorships_data(datafile)


dict_papers = {}
dict_authors = {}
start_time = time.time()
i = 1
for paper in papers:
    dict_papers[paper['id']] = i 
    paper['id'] = i
    i += 1
    paper['title'] = paper['title'].replace('\'','')
    create_queries.create_paper(gremlin_conn,paper['id'],paper['title'],paper['year'],paper['n_citation'])


i = 1
for author in authors:
    dict_authors[author['id']] = i
    author['id'] = i
    i +=1
    author['name'] = author['name'].replace('\'','')
    if 'org' in author.keys():
        author['org'] = author['org'].replace('\'','')
    else:
        author['org'] = ' '
    create_queries.create_author(gremlin_conn,author['id'],author['name'],author['org'])

for ref in refs:
    if (ref['from'] in dict_papers.keys() and ref['to'] in dict_papers.keys()):
        create_queries.create_reference(gremlin_conn,dict_papers[ref['from']],dict_papers[ref['to']])

for auth in auths:
    if (auth['author'] in dict_authors.keys() and auth['paper'] in dict_papers.keys()):
        create_queries.create_authorship(gremlin_conn,dict_authors[auth['author']],dict_papers[auth['paper']])

print(time.time()-start_time)
