def find_paper(self, id):
    """
    Return paper in search.
    """
    def find_paper_tx(tx, id):
        query = (
            "MATCH (p:Paper)"
            "WHERE p.id = $id "
            "RETURN p.id AS id"
        )
        result = tx.run(query, id=id)
        return [row['id'] for row in result]
    
    with self.driver.session(database="neo4j") as session:
        result = session.execute_read(find_paper_tx, id)
        print(result)


def find_author(self, id):
    """
    Return author in search.
    """
    def find_author_tx(tx, id):
        query = (
            "MATCH (a:Author)"
            "WHERE a.id = $id "
            "RETURN a.id AS id"
        )
        result = tx.run(query, id=id)
        return [row['id'] for row in result]
    
    with self.driver.session(database="neo4j") as session:
        result = session.execute_read(find_author_tx, id)
        print(result)


def references_of(self, id):
    """
    Return papers referenced by the argument paper.
    """
    def references_of_tx(tx, id):
        query = (
            "MATCH (p1:Paper)-[r]->(p2:Paper) "
            "WHERE p1.id = $id "
            "RETURN DISTINCT p2.id AS id"
        )
        result = tx.run(query, id=id)
        return [row['id'] for row in result]
    
    with self.driver.session(database="neo4j") as session:
        result = session.execute_read(references_of_tx, id)
        print(result)


def references_to(self, id):
    """
    Return papers that make reference to the argument paper.
    """
    def references_to_tx(tx, id):
        query = (
            "MATCH (p1:Paper)-[r]->(p2:Paper) "
            "WHERE p2.id = $id "
            "RETURN DISTINCT p1.id AS id"
        )
        result = tx.run(query, id=id)
        return [row['id'] for row in result]
    
    with self.driver.session(database="neo4j") as session:
        result = session.execute_read(references_to_tx, id)
        print(result)


def papers_of(self, id):
    """
    Return papers authored by the argument author.
    """
    def papers_of_tx(tx, id):
        query = (
            "MATCH (a:Author)-[r]->(p:Paper) "
            "WHERE a.id = $id "
            "RETURN DISTINCT p.id AS id"
        )
        result = tx.run(query, id=id)
        return [row['id'] for row in result]
    
    with self.driver.session(database="neo4j") as session:
        result = session.execute_read(papers_of_tx, id)
        print(result)


def authors_of(self, id):
    """
    Return authors of the argument paper.
    """
    def authors_of_tx(tx, id):
        query = (
            "MATCH (a:Author)-[r]-(p:Paper) "
            "WHERE p.id = $id "
            "RETURN DISTINCT a.id AS id"
        )
        result = tx.run(query, id=id)
        return [row['id'] for row in result]
    
    with self.driver.session(database="neo4j") as session:
        result = session.execute_read(authors_of_tx, id)
        print(result)