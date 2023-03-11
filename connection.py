"""
Contains connection classes for both databases 
and should be imported in order to connect.
"""
from time import time
from tornado import httpclient
from neo4j import GraphDatabase
from gremlin_python.driver import client

import queries.create_neo as create_neo
import queries.read_neo as read_neo
import queries.update_neo as update_neo
import queries.delete_neo as delete_neo
import queries.create_janus as create_janus
import queries.read_janus as read_janus
import queries.update_janus as update_janus
import queries.delete_janus as delete_janus


def transact_and_time(transaction, *args):
    """
    Run a transaction and return its duration.
    """
    start_time = time()
    transaction(*args)
    duration = time() - start_time
    return {transaction.__name__: duration}


class Neo4jConnection:
    """
    Handles connection to Neo4j and contains methods for transactions.
    """
    def __init__(self, uri, user, password, instance):
        self.instance = instance
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # CREATE ========================================================
    # Single
    create_paper = create_neo.create_paper
    create_author = create_neo.create_author
    create_reference = create_neo.create_reference
    create_authorship = create_neo.create_authorship

    #create_people = create_neo.create_people

    # Batch
    create_papers = create_neo.create_papers
    create_authors = create_neo.create_authors
    create_references = create_neo.create_references
    create_authorships = create_neo.create_authorships


    # Constraints
    paper_constraints = create_neo.paper_constraints
    author_constraints = create_neo.author_constraints
    
    # READ ========================================================
    # Simple Queries
    find_paper = read_neo.find_paper
    find_author = read_neo.find_author
    title_of_paper = read_neo.title_of_paper
    org_of_author = read_neo.org_of_author

    # Adjacency Queries
    references_of = read_neo.references_of
    references_to = read_neo.references_to
    papers_of = read_neo.papers_of
    authors_of = read_neo.authors_of

    # Reachability Queries
    are_collaborators = read_neo.are_collaborators

    # Analytical Queries
    mean_authors_per_paper = read_neo.mean_authors_per_paper

    # UPDATE ========================================================
    rename_paper = update_neo.rename_paper
    change_org = update_neo.change_org

    # DELETE ========================================================
    # Single
    delete_paper = delete_neo.delete_paper
    delete_author = delete_neo.delete_author
    delete_reference = delete_neo.delete_reference
    delete_authorship = delete_neo.delete_authorship

    # Clear
    clear_database = delete_neo.clear_database
    remove_constraints = delete_neo.remove_constraints


class JanusGraphConnection:
    """
    Handles connection to JanusGraph and contains methods for transactions.
    """
    def __init__(self, uri, scale):
        self.scale = scale
        ws_conn = httpclient.HTTPRequest(uri)
        self.gremlin_conn = client.Client(ws_conn, "g")
        self.gremlin_conn.submit("graph = TinkerGraph.open()")
        self.gremlin_conn.submit("g = graph.traversal()")

    def load_graph(self):
        # Load a GraphSON file, if it exists
        try:
            self.gremlin_conn.submit(f"graph.io(graphson()).readGraph('/home/dataset_{self.scale}.json')").next()
        except Exception as e:
            print(e)

    def query(self, q):
        # For debugging
        try:
            res = self.gremlin_conn.submit(q).next()
        except Exception as e:
            res = e
        return res

    def close(self):
        pass

    # CREATE ========================================================
    # Single
    create_paper = create_janus.create_paper
    create_author = create_janus.create_author
    create_reference = create_janus.create_reference
    create_authorship = create_janus.create_authorship

    # Batch
    # create_papers = create_janus.create_papers
    # create_authors = create_janus.create_authors
    # create_references = create_janus.create_references
    # create_authorships = create_janus.create_authorships

    # TODO atsorvat:
    # Constraints
    # paper_constraints = create_janus.paper_constraints
    # author_constraints = create_janus.author_constraints
    
    # READ ========================================================
    # Simple Queries
    find_paper = read_janus.find_paper
    find_author = read_janus.find_author
    title_of_paper = read_janus.title_of_paper
    org_of_author = read_janus.org_of_author

    # Adjacency Queries
    references_of = read_janus.references_of
    references_to = read_janus.references_to
    papers_of = read_janus.papers_of
    authors_of = read_janus.authors_of

    # Reachability Queries
    are_collaborators = read_janus.are_collaborators

    # Analytical Queries
    # TODO atsorvat:
    # mean_authors_per_paper = read_janus.mean_authors_per_paper

    # UPDATE ========================================================
    rename_paper = update_janus.rename_paper
    change_org = update_janus.change_org

    # DELETE ========================================================
    # Single
    delete_paper = delete_janus.delete_paper
    delete_author = delete_janus.delete_author
    delete_reference = delete_janus.delete_reference
    delete_authorship = delete_janus.delete_authorship

    # Clear
    clear_database = delete_janus.clear_database
    # remove_constraints = delete_janus.remove_constraints
