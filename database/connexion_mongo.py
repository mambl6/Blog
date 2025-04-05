from pymongo import MongoClient

def connexion_mongo(): 
    client = "mongodb+srv://BrayaneLaugane2001:BrayaneLaugane2001@cluster.aqxgh9e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster"
    return MongoClient(client)   

