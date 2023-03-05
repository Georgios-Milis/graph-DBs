def rename_paper(self, id, new_title):
    query = f"g.V().has('paper', 'id', {id}).property('title', '{new_title}')"
    try:
        response = self.submit(query).next()
    except Exception as e:
        response = e
    return response


def change_org(self, id, new_org):
    query = f"g.V().has('author', 'id', {id}).property('org', '{new_org}')"
    try:
        response = self.submit(query).next()
    except Exception as e:
        response = e
    return response
