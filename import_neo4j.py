# =============================================================================
# Fichier : import_neo4j.py
# Description : Importation des données de MongoDB vers Neo4j.
#               Ce script se connecte à Neo4j et crée les nœuds et relations nécessaires
#               (films, acteurs, réalisateurs, genres, et liaison des membres du projet).
# =============================================================================

from database.connexion_neo4j import connexion_neo4j  # Import de la fonction de connexion à Neo4j
from requete.requete_mongo import exporter_donnees_pour_neo4j  # Import de la fonction d'exportation depuis MongoDB
from neo4j import GraphDatabase  # Import des outils pour interagir avec Neo4j
import time  # Import du module temps pour mesurer la durée d'exécution

class Neo4jImporter:
    def __init__(self):
        # Établissement de la connexion à Neo4j
        self.driver = connexion_neo4j()
        # Exportation des données nécessaires depuis MongoDB
        self.data = exporter_donnees_pour_neo4j()
        # Liste des membres du projet à lier à un film de référence
        self.membres_projet = [
            "Brayane Mba Abessolo",
            "Laugane Metogo"
        ]
        # Film de référence pour lier les membres du projet
        self.film_reference = "Inception"

    def clean_name(self, name):
        """Nettoie les noms d'acteurs/réalisateurs en supprimant les espaces superflus et en mettant en majuscule la première lettre."""
        return name.strip().title()

    def importer_donnees(self):
        """Fonction principale pour l'import des données avec gestion d'erreurs.
        - Nettoie la base existante
        - Importe chaque film avec ses informations
        - Lie les membres du projet au film de référence
        Retourne le nombre de films importés avec succès.
        """
        print(f"Début de l'import de {len(self.data)} films...")
        start_time = time.time()
        success_count = 0

        with self.driver.session() as session:
            # Nettoyage initial de la base Neo4j
            print("Nettoyage de la base existante...")
            session.run("MATCH (n) DETACH DELETE n")

            # Import des films et des réalisateurs
            print("Import des films et réalisateurs...")
            for i, movie in enumerate(self.data, 1):
                try:
                    # Validation des données obligatoires
                    if not all(key in movie for key in ['_id', 'title', 'Director', 'Actors']):
                        print(f"Film {i} ignoré - données incomplètes")
                        continue

                    # Conversion sécurisée des données, ajout du champ genre
                    processed_movie = {
                        '_id': str(movie['_id']),
                        'title': str(movie['title']).strip(),
                        'year': int(movie.get('year', 0)) if str(movie.get('year', '0')).isdigit() else 0,
                        'Votes': int(movie.get('Votes', 0)) if str(movie.get('Votes', '0')).isdigit() else 0,
                        'Revenue (Millions)': movie.get('Revenue (Millions)'),
                        'rating': movie.get('rating', 'unrated'),
                        'Director': str(movie['Director']).strip(),
                        'Actors': str(movie['Actors']),
                        'genre': str(movie.get('genre', '')).strip()
                    }

                    # Exécution de l'import pour le film courant
                    session.execute_write(self._importer_film, movie=processed_movie)
                    success_count += 1

                except Exception as e:
                    print(f"Erreur sur le film {i} ({movie.get('_id', '?')} - {movie.get('title', 'Sans titre')}): {str(e)}")
                    continue

                # Affichage d'un rapport tous les 10 films traités
                if i % 10 == 0:
                    print(f"{i} films traités (dont {success_count} réussis)...")

            # Lier les membres du projet au film de référence
            print("Ajout des membres du projet...")
            session.execute_write(self._lier_membres_projet)

        duration = time.time() - start_time
        print(f"Import terminé en {duration:.2f} secondes")
        print(f"Résumé : {success_count} films importés avec succès sur {len(self.data)}")
        return success_count

    @staticmethod
    def _importer_film(tx, movie):
        """Exécute l'import d'un film dans Neo4j, incluant :
        - La création du nœud Film et l'association du réalisateur via la relation A_REALISE.
        - L'import des acteurs et la création de la relation A_JOUE_DANS.
        - La création d'une relation A_TRAVAILLE_AVEC entre le réalisateur et chaque acteur.
        - L'import des genres et la création de la relation A_POUR_GENRE.
        """
        try:
            # Préparation des acteurs (séparation par virgule, suppression des espaces)
            actors = [a.strip().title() for a in movie['Actors'].split(",") if a.strip()]

            # Gestion spéciale du revenu : conversion en float si possible
            revenue = None
            if movie['Revenue (Millions)'] not in [None, ""]:
                try:
                    revenue = float(str(movie['Revenue (Millions)']).replace(',', '.'))
                except (ValueError, TypeError):
                    pass

            # Création du nœud Film et du nœud Réalisateur avec la relation A_REALISE
            tx.run("""
            MERGE (f:Film {id: $id})
            SET f.title = $title,
                f.year = $year,
                f.votes = $votes,
                f.revenue = $revenue,
                f.rating = $rating
            MERGE (d:Realisateur {name: $director})
            MERGE (d)-[:A_REALISE]->(f)
            """, {
                'id': movie['_id'],
                'title': movie['title'],
                'year': movie['year'],
                'votes': movie['Votes'],
                'revenue': revenue,
                'rating': movie['rating'],
                'director': movie['Director']
            })

            # Import des acteurs et création de la relation A_JOUE_DANS
            if actors:
                tx.run("""
                UNWIND $actors AS actor
                MATCH (f:Film {id: $id})
                MERGE (a:Acteur {name: actor})
                MERGE (a)-[:A_JOUE_DANS]->(f)
                """, {
                    'id': movie['_id'],
                    'actors': actors
                })

                # Création de la relation entre le réalisateur et chaque acteur (A_TRAVAILLE_AVEC)
                tx.run("""
                MATCH (d:Realisateur {name: $director}), (f:Film {id: $id})
                MATCH (a:Acteur)-[:A_JOUE_DANS]->(f)
                MERGE (d)-[:A_TRAVAILLE_AVEC]->(a)
                """, {
                    'director': movie['Director'],
                    'id': movie['_id']
                })

            # Import des genres : création des nœuds Genre et liaison via la relation A_POUR_GENRE
            if "genre" in movie and movie["genre"]:
                genres_list = [g.strip() for g in movie["genre"].split(",") if g.strip()]
                if genres_list:
                    tx.run("""
                    UNWIND $genres AS genre
                    MATCH (f:Film {id: $id})
                    MERGE (g:Genre {name: genre})
                    MERGE (f)-[:A_POUR_GENRE]->(g)
                    """, {
                        'id': movie['_id'],
                        'genres': genres_list
                    })

        except Exception as e:
            print(f"Erreur lors de l'import du film {movie.get('_id')}: {str(e)}")
            raise  # Propagation de l'exception pour que execute_write détecte l'échec

    def _lier_membres_projet(self, tx):
        """Lie les membres du projet (type 'membre_projet') au film de référence (self.film_reference)."""
        for membre in self.membres_projet:
            tx.run("""
            MERGE (m:Acteur {name: $name, type: 'membre_projet'})
            WITH m
            MATCH (f:Film {title: $title})
            MERGE (m)-[:A_JOUE_DANS]->(f)
            """, {
                "name": membre,
                "title": self.film_reference
            })

    def close(self):
        """Ferme la connexion à Neo4j."""
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ----------------------------
    # Méthode Q14 : Acteur le plus prolifique
    # ----------------------------
    def acteur_plus_prolifique(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Acteur)-[:A_JOUE_DANS]->(f:Film)
                RETURN a.name AS acteur, COUNT(f) AS nb_films
                ORDER BY nb_films DESC
                LIMIT 1
            """)
            return result.single().data()


# =============================================================================
# Point d'entrée de l'importation (exécuté directement si le script est lancé)
# =============================================================================
if __name__ == "__main__":
    try:
        importer = Neo4jImporter()
        importer.importer_donnees()
    except Exception as e:
        print(f"Erreur lors de l'import : {str(e)}")
    finally:
        importer.close()
