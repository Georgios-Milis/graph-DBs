from neo4j import GraphDatabase

import create_queries
import read_queries
import update_queries
import delete_queries


class Connection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    create_paper = create_queries.create_paper
    create_author = create_queries.create_author
    create_reference = create_queries.create_reference
    create_authorship = create_queries.create_authorship

    create_papers = create_queries.create_papers

    find_paper = read_queries.find_paper
    find_author = read_queries.find_author
    references_of = read_queries.references_of
    references_to = read_queries.references_to
    papers_of = read_queries.papers_of
    authors_of = read_queries.authors_of

    rename_paper = update_queries.rename_paper

    delete_paper = delete_queries.delete_paper
    delete_author = delete_queries.delete_author
    delete_reference = delete_queries.delete_reference
    delete_authorship = delete_queries.delete_authorship
    clear_database = delete_queries.clear_database
