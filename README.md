# Projet NoSQL – Exploration et Interrogation de Bases de Données NoSQL avec Python

Ce projet a pour objectif d'explorer et d'interroger des bases de données NoSQL en utilisant MongoDB et Neo4j, tout en fournissant une interface interactive développée avec Streamlit. Il permet d'analyser une collection de films et de visualiser diverses statistiques et relations (acteurs, réalisateurs, genres, etc.).

## Prérequis

- Python 3.x
- Git
- MongoDB - Base de données NoSQL orientée documents
- Neo4j - Base de données NoSQL orientée graphes
- Streamlit - Framework pour créer des interfaces web interactives
- PyMongo - Client Python pour interagir avec MongoDB

## Étapes d'installation

**1. Cloner le dépôt GitHub :**

- git clone https://github.com/votre-utilisateur/votre-repo.git
- cd votre-repo

**2. Créer et activer un environnement virtuel :**

**Sur Windows :**

- python -m venv venv
- venv\Scripts\activate

**Sur macOS/Linux :**
- python -m venv venv
- source venv/bin/activate

**3. Installer les dépendances :**
- pip install -r requirements.txt

**4. Utilisation**

- Lancer l'application
Pour démarrer l'application, assurez-vous d'abord que votre environnement virtuel est activé, puis exécutez :

- streamlit run app.py

Cela ouvrira une interface web dans votre navigateur où vous pourrez :
- **Interroger la base MongoDB pour analyser les films.**

- **Explorer le graphe Neo4j avec diverses requêtes.**

- **Visualiser des graphiques et tableaux interactifs.**


## Fonctionnalités

- **MongoDB :**
  - Importation des données de films depuis un fichier JSON
  - Exécution de requêtes pour extraire des informations telles que :
    - Année avec le plus grand nombre de films.
    - Nombre de films sortis après 1999.
    - Moyenne des votes des films.
    - Histogramme du nombre de films par année.
    - Extraction des genres, etc.

- **Neo4j :**
  - Importation des données depuis MongoDB pour créer un graphe de films, acteurs, réalisateurs et genres.
  - Requêtes Cypher pour explorer le graphe, telles que :
    - Trouver l'acteur le plus prolifique.
    - Chemin le plus court entre deux acteurs.
    - Analyse des communautés d'acteurs (algorithme Louvain).
    - Recommandations de films basées sur les préférences d'un acteur.
    - Relations d'influence et concurrence entre réalisateurs, etc.

- **Interface Streamlit :**
  - Interface utilisateur interactive permettant d'exécuter les requêtes et de visualiser les résultats sous forme de tableaux et graphiques.
![Capture d'écran 2025-04-06 110610](https://github.com/user-attachments/assets/33b7f74b-8e22-4315-bf73-89c8d0b1d6a9)
![Capture d'écran 2025-04-06 110641](https://github.com/user-attachments/assets/8739d141-abbf-477c-9fab-7f9428b19f86)


    

## Structure du Projet

```plaintext
PROJET_NOSQL
├── __pycache__                # Fichiers de cache Python (générés automatiquement)
├── database
│   ├── connexion_mongo.py     # Connexion à la base MongoDB
│   └── connexion_neo4j.py     # Connexion à la base Neo4j
├── requete
│   ├── requete_mongo.py       # Requêtes pour interroger MongoDB
│   └── requete_neo4j.py       # Requêtes pour interroger Neo4j
├── venv                       # Environnement virtuel Python
├── app.py                     # Application principale Streamlit
├── correlation_duree_revenu.png  # Exemple de graphique généré pour la corrélation durée-revenu
├── import_neo4j.py            # Script d'importation des données de MongoDB vers Neo4j
└── requirements.txt           # Liste des dépendances du projet
