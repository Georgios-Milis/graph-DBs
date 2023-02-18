def rename_paper(self, id, title):
    """
    Return paper in search.
    """
    def rename_paper_tx(tx, id, title):
        query = (
            "MATCH (p:Paper)"
            "WHERE p.id = $id "
            "SET p.title = $title"
        )
        result = tx.run(query, id=id, title=title)
        return [row['id'] for row in result]
    
    with self.driver.session(database=self.instance) as session:
        result = session.execute_read(rename_paper_tx, id, title)
        print(result)
