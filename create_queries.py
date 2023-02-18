import logging
from neo4j.exceptions import ServiceUnavailable


def create_paper(self, attributes):
    """
    Create and return a paper.
    """
    def create_paper_tx(tx, attributes):
        query = (
            "CREATE (p:Paper $attributes) "
        )
        result = tx.run(query, attributes=attributes)
        return result
        # try:
        #     return [{'p': row['p']['id']} for row in result]
        # except ServiceUnavailable as exception:
        #     logging.error(f"{query} raised an error: \n {exception}")
        #     raise

    with self.driver.session(database=self.instance) as session:
        result = session.execute_write(create_paper_tx, attributes)
        # for row in result: print(f"Created paper: {row['p']}")


def create_papers(self, papers):
    """
    Insert all papers to database.
    """
    def create_papers_tx(tx, papers):
        query = (
            """
            UNWIND $papers AS map
            CREATE (p:Paper)
            SET p = map
            """
        )
        tx.run(query, papers=papers)

    with self.driver.session(database=self.instance) as session:
        session.execute_write(create_papers_tx, papers)


def create_authors(self, authors):
    """
    Insert all papers to database.
    """
    def create_authors_tx(tx, authors):
        query = (
            """
            UNWIND $authors AS map
            CREATE (a:Author)
            SET a = map
            """
        )
        tx.run(query, authors=authors)

    with self.driver.session(database=self.instance) as session:
        session.execute_write(create_authors_tx, authors)


def create_author(self, attributes):
    """
    Create and return an author.
    """
    def create_author_tx(tx, attributes):
        query = (
            "CREATE (a:Author $attributes) "
            "RETURN a"
        )
        result = tx.run(query, attributes=attributes)
        try:
            return [{'a': row['a']['id']} for row in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")
            raise

    with self.driver.session(database=self.instance) as session:
        result = session.execute_write(create_author_tx, attributes)
        for row in result: print(f"Created author: {row['a']}")


def create_reference(self, id, ref_id):
    """
    Create a reference.
    """
    def create_reference_tx(tx, id, ref_id):
        query = (
            """
            MATCH (p1:Paper), (p2:Paper)
            WHERE p1.id = $id AND p2.id = $ref_id
            CREATE (p1)-[r:REFERENCE]->(p2)
            RETURN p1, p2
            """
        )
        result = tx.run(query, id=id, ref_id=ref_id)
        try:
            return [{'p1': row['p1']['id'], 'p2': row['p2']['id']} for row in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")
            raise

    with self.driver.session(database=self.instance) as session:
        result = session.execute_write(create_reference_tx, id, ref_id)
        for row in result: print(f"Created reference of {row['p1']['id']} to {row['p2']['id']}.")


def create_references(self, references):
    """
    Insert all references to database.
    """
    def create_references_tx(tx, references):
        query = (
            """
            UNWIND $references AS edge
            MATCH (p1:Paper), (p2:Paper)
            WHERE p1.id = edge.from AND p2.id = edge.to
            CREATE (p1)-[r:REFERENCE]->(p2)
            """  
        )
        tx.run(query, references=references)

    with self.driver.session(database=self.instance) as session:
        session.execute_write(create_references_tx, references)


def create_authorship(self, author_id, paper_id):
    """
    Create an authorship.
    """
    def create_authorship_tx(tx, author_id, paper_id):
        query = (
            """
            MATCH (a:Author), (p:Paper)
            WHERE a.id = $author_id AND p.id = $paper_id
            CREATE (a)-[r:AUTHORSHIP]-(p)
            RETURN a, p
            """
        )
        result = tx.run(query, author_id=author_id, paper_id=paper_id)
        try:
            return [{'a': row['a']['id'], 'p': row['p']['id']} for row in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")
            raise

    with self.driver.session(database=self.instance) as session:
        result = session.execute_write(create_authorship_tx, author_id, paper_id)
        for row in result: print(f"Created authorship between {row['a']['id']} and {row['p']['id']}.")


def create_authorships(self, authorships):
    """
    Insert all references to database.
    """
    def create_authorships_tx(tx, authorships):
        query = (
            """
            UNWIND $authorships AS edge
            MATCH (a:Author), (p:Paper)
            WHERE a.id = edge.author AND p.id = edge.paper
            CREATE (a)-[r:AUTHORSHIP]->(p)
            """  
        )
        tx.run(query, authorships=authorships)

    with self.driver.session(database=self.instance) as session:
        session.execute_write(create_authorships_tx, authorships)
