from time import time
from neo4j import GraphDatabase

import create_queries
import read_queries
import update_queries
import delete_queries


def transact_and_time(transaction, arg=None):
    """
    Run a transaction and return its duration.
    """
    start_time = time()
    transaction(arg) if arg != None else transaction()
    duration = time() - start_time
    return {transaction.__name__: duration}


class Connection:
    def __init__(self, uri, user, password, instance):
        self.instance = instance
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # Single CREATE
    create_paper = create_queries.create_paper
    create_author = create_queries.create_author
    create_reference = create_queries.create_reference
    create_authorship = create_queries.create_authorship
    # Batch CREATE
    create_papers = create_queries.create_papers
    create_authors = create_queries.create_authors
    create_references = create_queries.create_references
    create_authorships = create_queries.create_authorships
    
    # READ
    find_paper = read_queries.find_paper
    find_author = read_queries.find_author
    references_of = read_queries.references_of
    references_to = read_queries.references_to
    papers_of = read_queries.papers_of
    authors_of = read_queries.authors_of

    # UPDATE
    rename_paper = update_queries.rename_paper

    # DELETE
    delete_paper = delete_queries.delete_paper
    delete_author = delete_queries.delete_author
    delete_reference = delete_queries.delete_reference
    delete_authorship = delete_queries.delete_authorship
    # Clear
    clear_database = delete_queries.clear_database
