def rename_paper(gremlin_conn,id,new_title):
    query = f"g.V().has('paper', 'id', {id}).property('title', '{new_title}')"
    try:
        response = gremlin_conn.submit(query).next()
    except:
        response = "Something went wrong with vertice create" 
    return response

def change_org(gremlin_conn,id,new_org):
    query = f"g.V().has('author', 'id', {id}).property('org', '{new_org}')"
    try:
        response = gremlin_conn.submit(query).next()
    except:
        response = "Something went wrong with vertice create" 
    return response