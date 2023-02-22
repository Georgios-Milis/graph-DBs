from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection


connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
g = traversal().withRemote(connection)

# TESTED WITH g ON CONSOLE:
# g = traversal().withEmbedded(TinkerFactory.createTheCrew())

test = g.V().hasLabel('person').values().next()#.valueMap()
print(test)

connection.close()
