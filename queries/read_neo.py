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
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(find_paper_tx, id)
        return result


def title_of_paper(self, id):
    """
    Return the title of the paper with given id.
    """
    def title_of_paper_tx(tx, id):
        query = (
            "MATCH (p:Paper)"
            "WHERE p.id = $id "
            "RETURN p.title AS title"
        )
        result = tx.run(query, id=id)
        return [row['title'] for row in result]
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(title_of_paper_tx, id)
        return result
    

def org_of_author(self, id):
    """
    Return the title of the paper with given id.
    """
    def org_of_author_tx(tx, id):
        query = (
            "MATCH (a:Author)"
            "WHERE a.id = $id "
            "RETURN a.org AS org"
        )
        result = tx.run(query, id=id)
        return [row['org'] for row in result]
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(org_of_author_tx, id)
        return result


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
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(find_author_tx, id)
        return result


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
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(references_of_tx, id)
        return result


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
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(references_to_tx, id)
        return result


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
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(papers_of_tx, id)
        return result


def mean_authors_per_paper(self):
    """
    Analytical query
    """
    def mean_authors_per_paper_tx(tx):
        query = (
            """
            CALL apoc.stats.degrees("AUTHORSHIP");
            """
        )
        result = tx.run(query)
        return [row['mean'] for row in result][0]
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(mean_authors_per_paper_tx)
        return result


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
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(authors_of_tx, id)
        return result


def are_collaborators(self, id1, id2):
    """
    Reachability query
    """
    def are_collaborators_tx(tx, id1, id2):
        query = (
            """
            MATCH (a1:Author)-[:AUTHORSHIP]-(p1:Paper), (a2:Author)-[:AUTHORSHIP]-(p2:Paper)
            WHERE p1.id = p2.id AND a1.id = $id1 AND a2.id = $id2
            RETURN p1.id AS id
            """
        )
        result = tx.run(query, id1=id1, id2=id2)
        return [row['id'] for row in result]
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(are_collaborators_tx, id1, id2)
        return result
