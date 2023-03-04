"""
Contains the connection to neo4j databse and should 
be imported in order to connect.
"""
from time import time
from neo4j import GraphDatabase

import queries.create_queries as create_queries
import queries.read_queries as read_queries
import queries.update_queries as update_queries
import queries.delete_queries as delete_queries


def transact_and_time(transaction, *args):
    """
    Run a transaction and return its duration.
    """
    start_time = time()
    transaction(*args)
    duration = time() - start_time
    return {transaction.__name__: duration}


class Connection:
    """
    Handles connection to neo4j and contains methods for transactions.
    """
    def __init__(self, uri, user, password, instance):
        self.instance = instance
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # CREATE ========================================================
    # Single
    create_paper = create_queries.create_paper
    create_author = create_queries.create_author
    create_reference = create_queries.create_reference
    create_authorship = create_queries.create_authorship

    # Batch
    create_papers = create_queries.create_papers
    create_authors = create_queries.create_authors
    create_references = create_queries.create_references
    create_authorships = create_queries.create_authorships

    # Constraints

    paper_constraints = create_queries.paper_constraints
    author_constraints = create_queries.author_constraints
    
    # READ ========================================================
    # Simple Queries
    find_paper = read_queries.find_paper
    find_author = read_queries.find_author
    title_of_paper = read_queries.title_of_paper
    org_of_author = read_queries.org_of_author

    # Adjacency Queries
    references_of = read_queries.references_of
    references_to = read_queries.references_to
    papers_of = read_queries.papers_of
    authors_of = read_queries.authors_of

    # Reachability Queries
    are_collaborators = read_queries.are_collaborators

    # Analytical Queries
    mean_authors_per_paper = read_queries.mean_authors_per_paper


    # UPDATE ========================================================
    rename_paper = update_queries.rename_paper
    change_org = update_queries.change_org


    # DELETE ========================================================
    # Single
    delete_paper = delete_queries.delete_paper
    delete_author = delete_queries.delete_author
    delete_reference = delete_queries.delete_reference
    delete_authorship = delete_queries.delete_authorship

    # Clear
    clear_database = delete_queries.clear_database
    remove_constraints = delete_queries.remove_constraints
