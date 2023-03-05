def rename_paper(self, id, title):
    """
    Rename a paper with given id
    """
    def rename_paper_tx(tx, id, title):
        query = (
            """
            MATCH (p:Paper)
            WHERE p.id = $id
            SET p.title = $title
            """
        )
        return tx.run(query, id=id, title=title)
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_write(rename_paper_tx, id, title)
        return result


def change_org(self, id, org):
    """
    Change the organization of the author with given id 
    """
    def change_org_tx(tx, id, org):
        query = (
            """
            MATCH (a:Author)
            WHERE a.id = $id
            SET a.org = $org
            """
        )
        return tx.run(query, id=id, org=org)
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_write(change_org_tx, id, org)
        return result
