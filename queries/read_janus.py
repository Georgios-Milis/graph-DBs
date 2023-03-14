def get_vertices(self):
    query = f"g.V().values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def get_papers(self, samples=None):
    if samples is None:
        query = f"g.V().hasLabel('paper').values('id')"
    else:
        query = f"g.V().hasLabel('paper').limit({samples}).values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def find_paper(self, id):
    query = f"g.V().has('paper', 'id', '{id}').values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def find_author(self, id):
    query = f"g.V().has('author', 'id', '{id}').values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def title_of_paper(self, id):
    query = f"g.V().has('paper', 'id', '{id}').values('title')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No paper with id: {id}"
    return response


def org_of_author(self, id):
    query = f"g.V().has('author', 'id', '{id}').values('org')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No author with id: {id}"
    return response


def references_of(self, id):
    query = f"g.V().has('paper', 'id', '{id}').out('reference').values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def references_to(self, id):
    query = f"g.V().has('paper', 'id', '{id}').in('reference').values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def papers_of(self, id):
    query = f"g.V().has('author', 'id', '{id}').out('authorship').values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def authors_of(self, id):
    query = f"g.V().has('paper', 'id', '{id}').in('authorship').values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def are_collaborators(self, id1, id2):
    query = f"g.V().and(inE('authorship').where(otherV().has('author', 'id', '{id1}')), inE('authorship').where(otherV().has('author', 'id', '{id2}'))).values('id')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except StopIteration:
        response = []
    return response


def mean_authors_per_paper(self, samples=None):
    # Use samples for faster execution
    try:
        from numpy import mean
        if samples is None:
            response = mean([len(self.authors_of(paper_id)) for paper_id in self.get_papers()])
        else:
            response = mean([len(self.authors_of(paper_id)) for paper_id in self.get_papers(samples)])
    except Exception as e:
        response = e
    return response
