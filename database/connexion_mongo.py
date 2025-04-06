# Importation de MongoClient depuis pymongo pour se connecter à la base de données MongoDB
from pymongo import MongoClient

def connexion_mongo():
    # Chaîne de connexion vers MongoDB Atlas avec les identifiants
    client = "mongodb+srv://BrayaneLaugane2001:BrayaneLaugane2001@cluster.aqxgh9e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster"
    # Retourne l'objet client permettant d'interagir avec la base MongoDB
    return MongoClient(client)
