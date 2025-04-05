from neo4j import GraphDatabase

def connexion_neo4j():
    return GraphDatabase.driver("neo4j+s://276d7b92.databases.neo4j.io", auth=("neo4j", "4BU1U3LG5TXyYcJlxoWKzEBwgfmL06jx8qJJr864aOo"))

                                