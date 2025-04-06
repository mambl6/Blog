# =============================================================================
# Fichier : requete_neo4j.py
# Description : Module pour l'exécution des requêtes sur la base de données Neo4j
#               dans le cadre de l'analyse des films. Les fonctions sont appelées
#               via l'interface Streamlit.
# =============================================================================

from database.connexion_neo4j import connexion_neo4j  # Import de la connexion à Neo4j
from import_neo4j import Neo4jImporter  # Import pour pouvoir réimporter les données si nécessaire
import streamlit as st  # Import de Streamlit pour l'interface utilisateur
from neo4j import GraphDatabase  # Import pour interagir avec Neo4j
import pandas as pd  # Import de Pandas pour la manipulation des données dans Streamlit


class Neo4jManager:
    def __init__(self):
        # Établissement de la connexion à la base Neo4j
        self.driver = connexion_neo4j()
        
    def close(self):
        # Fermeture de la connexion Neo4j si elle existe
        if hasattr(self, 'driver'):
            self.driver.close()
    
    # -------------------------------------------------------------------------
    # Méthodes pour les requêtes sur Neo4j
    # -------------------------------------------------------------------------
    
    ## Méthode Q15 : Acteurs ayant joué avec Anne Hathaway
    def acteurs_avec_anne_hathaway(self):
        """
        Exécute une requête pour trouver les acteurs ayant joué dans les mêmes films qu'Anne Hathaway.
        Retourne une liste de dictionnaires contenant le nom du co-acteur, la liste des films communs
        et le nombre total de films en commun.
        """
        try:
            with self.driver.session() as session:
                result = session.run("""
                MATCH (anne:Acteur {name: 'Anne Hathaway'})-[:A_JOUE_DANS]->(f:Film)<-[:A_JOUE_DANS]-(coacteur:Acteur)
                WHERE coacteur <> anne
                RETURN DISTINCT coacteur.name AS coacteur, 
                       COLLECT(DISTINCT f.title) AS films,
                       COUNT(f) AS nb_films
                ORDER BY nb_films DESC, coacteur
                """)
                return [dict(record) for record in result]
        except Exception as e:
            st.error(f"Erreur Neo4j: {str(e)}")
            return []

    ## Méthode Q16 : Acteur avec le plus gros revenu total
    def acteur_plus_rentable(self):
        """
        Exécute une requête pour trouver l'acteur dont la somme des revenus des films auxquels il
        a participé est la plus élevée.
        Retourne un dictionnaire contenant le nom de l'acteur, le revenu total et le nombre de films.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Acteur)-[:A_JOUE_DANS]->(f:Film)
                WHERE f.revenue IS NOT NULL
                RETURN a.name AS acteur, 
                       SUM(toFloat(f.revenue)) AS revenu_total,
                       COUNT(f) AS nb_films
                ORDER BY revenu_total DESC
                LIMIT 1
            """)
            record = result.single()
            return dict(record) if record else None

    ## Méthode Q17 : Moyenne des votes
    def moyenne_votes(self):
        """
        Calcule la moyenne des votes des films.
        Retourne la moyenne des votes arrondie.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (f:Film)
                WHERE f.votes IS NOT NULL
                RETURN round(avg(toInteger(f.votes))) AS moyenne_votes
            """)
            record = result.single()
            return record["moyenne_votes"] if record else None

    ## Méthode Q18 : Genre le plus représenté
    def genre_plus_represente(self):
        """
        Trouve le genre de film le plus représenté dans la base en comptant le nombre de films 
        associés à chaque genre.
        Retourne un dictionnaire avec le nom du genre et le nombre de films.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (f:Film)-[:A_POUR_GENRE]->(g:Genre)
                RETURN g.name AS genre, COUNT(f) AS nb_films
                ORDER BY nb_films DESC
                LIMIT 1
            """)
            record = result.single()
            return dict(record) if record else None

    ## Méthode Q19 : Films des co-acteurs
    def films_coacteurs(self):
        """
        Trouve les films dans lesquels les co-acteurs d'un acteur donné ont joué.
        Retourne une liste de dictionnaires avec le titre du film et la liste des co-acteurs.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (moi:Acteur)-[:A_JOUE_DANS]->(mes_films:Film)
                MATCH (autres:Acteur)-[:A_JOUE_DANS]->(mes_films)
                WHERE autres <> moi
                WITH mes_films, collect(DISTINCT autres.name) AS coacteurs
                RETURN mes_films.title AS film, coacteurs
                ORDER BY mes_films.year DESC
                LIMIT 20
            """)
            return [dict(record) for record in result]
        
    ## Méthode Q20 : Réalisateur avec le plus d'acteurs distincts
    def realisateur_plus_acteurs_distincts(self):
        """
        Identifie le réalisateur ayant travaillé avec le plus grand nombre d'acteurs distincts.
        Retourne un dictionnaire avec le nom du réalisateur et le nombre d'acteurs distincts.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (d:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Acteur)
                RETURN d.name AS director, count(DISTINCT a.name) AS nb_acteurs
                ORDER BY nb_acteurs DESC
                LIMIT 1
            """)
            record = result.single()
            return dict(record) if record else None
    
    ## Méthode Q21 : Films les plus connectés
    def films_plus_connectes(self):
        """
        Trouve les films qui partagent le plus d'acteurs en commun avec d'autres films.
        Retourne une liste de dictionnaires avec le titre du film et le nombre d'acteurs communs.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (f:Film)<-[:A_JOUE_DANS]-(a:Acteur)-[:A_JOUE_DANS]->(f2:Film)
                WHERE f <> f2
                WITH f, count(DISTINCT a) AS commonActors
                RETURN f.title AS film, commonActors
                ORDER BY commonActors DESC
                LIMIT 5
            """)
            return [dict(record) for record in result]
    
    ## Méthode Q22 : Top 5 acteurs avec le plus de réalisateurs différents
    def top_5_acteurs_realisateurs(self):
        """
        Identifie les 5 acteurs ayant joué avec le plus de réalisateurs différents.
        Retourne une liste de dictionnaires avec le nom de l'acteur et le nombre de réalisateurs.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Acteur)-[:A_JOUE_DANS]->(f:Film)<-[:A_REALISE]-(d:Realisateur)
                RETURN a.name AS actor, count(DISTINCT d.name) AS nb_directeurs
                ORDER BY nb_directeurs DESC
                LIMIT 5
            """)
            return [dict(record) for record in result]
    
    ## Méthode Q23 : Recommander un film à un acteur
    def recommander_film_a_acteur(self, actorName):
        """
        Recommande un film à un acteur basé sur les genres des films dans lesquels il a déjà joué.
        Retourne un dictionnaire avec le titre du film, l'année et le nombre de votes.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Acteur {name: $actorName})-[:A_JOUE_DANS]->(f:Film)-[:A_POUR_GENRE]->(g:Genre)
                WITH a, collect(DISTINCT g) AS genres
                MATCH (rec:Film)-[:A_POUR_GENRE]->(g2:Genre)
                WHERE g2.name IN genres AND NOT (a)-[:A_JOUE_DANS]->(rec)
                RETURN rec.title AS film, rec.year AS year, rec.votes AS votes
                ORDER BY rec.votes DESC
                LIMIT 1
            """, {'actorName': actorName})
            record = result.single()
            return dict(record) if record else None

    ## Méthode Q24 : Créer relation INFLUENCE_PAR entre réalisateurs
    def creer_relation_influence(self, seuil=2):
        """
        Crée des relations INFLUENCE_PAR entre les réalisateurs si le nombre de genres communs entre leurs films
        est supérieur ou égal au seuil (par défaut 2).
        Retourne la liste des relations créées avec le nom des réalisateurs et le poids de l'influence.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (d1:Realisateur)-[:A_REALISE]->(f1:Film)-[:A_POUR_GENRE]->(g:Genre)<-[:A_POUR_GENRE]-(f2:Film)<-[:A_REALISE]-(d2:Realisateur)
                WHERE d1 <> d2
                WITH d1, d2, count(DISTINCT g) AS commonGenres
                WHERE commonGenres >= $seuil
                MERGE (d1)-[r:INFLUENCE_PAR]->(d2)
                SET r.weight = commonGenres
                RETURN d1.name AS director1, d2.name AS director2, r.weight AS influence
            """, {'seuil': seuil})
            return [dict(record) for record in result]
        
    ## Méthode Q25 : Chemin le plus court entre deux acteurs
    def chemin_plus_court(self, acteur1, acteur2):
        """
        Trouve et retourne le chemin le plus court entre deux acteurs spécifiés.
        Retourne le chemin sous forme d'objet (si trouvé).
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH p = shortestPath((a:Acteur {name: $acteur1})-[*]-(b:Acteur {name: $acteur2}))
                RETURN p
            """, {'acteur1': acteur1, 'acteur2': acteur2})
            return result.single()

    ## Méthode Q27 : Films avec genres en commun et réalisateurs différents
    def films_genres_communs_differents_realisateurs(self):
        """
        Trouve les films qui partagent un ou plusieurs genres en commun mais qui ont des réalisateurs différents.
        Retourne une liste de dictionnaires avec les titres des deux films et le genre commun.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (f1:Film)-[:A_POUR_GENRE]->(g:Genre)<-[:A_POUR_GENRE]-(f2:Film)
                WHERE f1 <> f2
                  AND f1.year = f2.year  // facultatif : comparaison pour la même année
                  AND NOT EXISTS {
                      MATCH (f1)<-[:A_REALISE]-(d1:Realisateur),
                            (f2)<-[:A_REALISE]-(d2:Realisateur)
                      WHERE d1 = d2
                  }
                RETURN f1.title AS film1, f2.title AS film2, g.name AS genre
                ORDER BY film1, film2
            """)
            return [dict(record) for record in result]
        
    ## Méthode Q28 : Recommander des films en fonction d'un acteur
    def recommander_films_utilisateur(self, actorName):
        """
        Recommande des films à un utilisateur en fonction des préférences d'un acteur donné.
        Retourne une liste de dictionnaires avec le titre, l'année et le nombre de votes des films recommandés.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Acteur {name: $actorName})-[:A_JOUE_DANS]->(f:Film)-[:A_POUR_GENRE]->(g:Genre)
                WITH a, collect(DISTINCT g.name) AS genres
                MATCH (rec:Film)-[:A_POUR_GENRE]->(g2:Genre)
                WHERE g2.name IN genres AND NOT (a)-[:A_JOUE_DANS]->(rec)
                RETURN rec.title AS film, rec.year AS year, rec.votes AS votes
                ORDER BY rec.votes DESC
                LIMIT 5
            """, {'actorName': actorName})
            return [dict(record) for record in result]
        
    ## Méthode Q29 : Créer relation CONCURRENCE entre réalisateurs
    def creer_relation_concurrence(self):
        """
        Crée des relations CONCURRENCE entre les réalisateurs ayant réalisé des films similaires
        la même année. Retourne une liste de dictionnaires contenant les noms des réalisateurs, l'année
        et le nombre de genres en commun.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (d1:Realisateur)-[:A_REALISE]->(f1:Film)-[:A_POUR_GENRE]->(g:Genre)<-[:A_POUR_GENRE]-(f2:Film)<-[:A_REALISE]-(d2:Realisateur)
                WHERE f1.year = f2.year AND d1 <> d2
                WITH d1, d2, f1.year AS year, count(DISTINCT g) AS commonGenres
                WHERE commonGenres > 0
                MERGE (d1)-[r:CONCURRENCE {year: year}]->(d2)
                SET r.commonGenres = commonGenres
                RETURN d1.name AS director1, d2.name AS director2, r.commonGenres AS commonGenres, r.year AS year
            """)
            return [dict(record) for record in result]
        
    ## Méthode Q30 : Collaborations fréquentes entre réalisateurs et acteurs
    def collaborations_realisateurs_acteurs(self):
        """
        Analyse les collaborations entre réalisateurs et acteurs, en calculant le nombre de films communs,
        ainsi que les revenus et les metascores moyens associés à ces collaborations.
        Retourne une liste de dictionnaires avec le nom du réalisateur, de l'acteur, le nombre de
        collaborations, le revenu moyen et le metascore moyen.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (d:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Acteur)
                WITH d, a, count(f) AS nb_collaborations, avg(f.revenue) AS avg_revenue, avg(f.Metascore) AS avg_metascore
                RETURN d.name AS director, a.name AS actor, nb_collaborations, avg_revenue, avg_metascore
                ORDER BY nb_collaborations DESC
                LIMIT 10
            """)
            return [dict(record) for record in result]


# =============================================================================
# Fonction requete_neo4j : point d'entrée pour l'interface Streamlit des requêtes Neo4j
# =============================================================================
def requete_neo4j():
    st.header("🔄 Requêtes Neo4j")
    
    # --- Section Import des données dans Neo4j ---
    with st.expander("🔽 Initialisation de la base Neo4j"):
        if st.button("1. Importer les données dans Neo4j"):
            with st.spinner("Import en cours..."):
                try:
                    with Neo4jImporter() as importer:
                        success_count = importer.importer_donnees()
                    
                    if success_count > 0:
                        st.success(f"Import réussi : {success_count} films importés")
                        with st.expander("Statistiques d'import"):
                            stats = {
                                'Total': len(importer.data),
                                'Réussis': success_count,
                                'Échecs': len(importer.data) - success_count
                            }
                            st.json(stats)
                    else:
                        st.error("Aucun film n'a pu être importé")
                        
                except Exception as e:
                    st.error(f"Erreur critique lors de l'import : {str(e)}")
    
    # --- Section des requêtes ---
    st.header("📊 Requêtes Neo4j")
    
    # Q14 : Acteur le plus prolifique
    if st.button("14 - Acteur le plus prolifique"):
        try:
            with Neo4jImporter() as importer:  
                result = importer.acteur_plus_prolifique()  
                if result:
                    st.success(f"🎭 {result['acteur']} a joué dans {result['nb_films']} films")
                else:
                    st.warning("Aucun résultat trouvé")
        except Exception as e:
            st.error(f"Erreur lors de la requête : {str(e)}")
            
    # Q15 : Acteurs ayant joué avec Anne Hathaway
    if st.button("15 - Acteurs ayant joué avec Anne Hathaway"):
        manager = Neo4jManager()
        results = manager.acteurs_avec_anne_hathaway()
        if not results:
            st.warning("Aucun co-acteur trouvé")
        else:
            for r in results:
                with st.expander(r['coacteur']):
                    st.write("Films en commun :", ", ".join(r['films']))
        manager.close()
                    
    # Q16 : Acteur avec le plus gros revenu total
    if st.button("16 - Acteur avec le plus gros revenu total"):
        manager = Neo4jManager()
        result = manager.acteur_plus_rentable()
        if result:
            st.success(f"💰 {result['acteur']} - Revenu total: ${result['revenu_total']:,.2f}M ({result['nb_films']} films)")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()

    # Q17 : Moyenne des votes
    if st.button("17 - Moyenne des votes"):
        manager = Neo4jManager()
        moyenne = manager.moyenne_votes()
        st.success(f"⭐ Moyenne des votes: {moyenne:,.0f}")
        manager.close()

    # Q18 : Genre le plus représenté
    if st.button("18 - Genre le plus représenté"):
        manager = Neo4jManager()
        result = manager.genre_plus_represente()
        if result:
            st.success(f"🎬 Genre dominant: {result['genre']} ({result['nb_films']} films)")
        else:
            st.warning("Aucun genre trouvé")
        manager.close()

    # Q19 : Films des co-acteurs
    if st.button("19 - Films des co-acteurs"):
        manager = Neo4jManager()
        results = manager.films_coacteurs()
        if not results:
            st.warning("Aucun film trouvé")
        else:
            st.success(f"🎥 {len(results)} films associés à vos co-acteurs")
            for item in results:
                with st.expander(item["film"]):
                    st.write("Avec:", ", ".join(item["coacteurs"]))
        manager.close()
    
    # Q20 : Réalisateur avec le plus d'acteurs distincts
    if st.button("20 - Réalisateur avec le plus d'acteurs distincts"):
        manager = Neo4jManager()
        result = manager.realisateur_plus_acteurs_distincts()
        if result:
            st.success(f"🎬 {result['director']} a travaillé avec {result['nb_acteurs']} acteurs distincts")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
    
    # Q21 : Films les plus connectés
    if st.button("21 - Films les plus connectés"):
        manager = Neo4jManager()
        results = manager.films_plus_connectes()
        if results:
            for res in results:
                st.write(f"Film : {res['film']} - Acteurs communs : {res['commonActors']}")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
        
    # Q22 : Top 5 acteurs avec le plus de réalisateurs différents
    if st.button("22 - Top 5 acteurs avec le plus de réalisateurs différents"):
        manager = Neo4jManager()
        results = manager.top_5_acteurs_realisateurs()
        if results:
            for res in results:
                st.write(f"Acteur : {res['actor']} - Nombre de réalisateurs : {res['nb_directeurs']}")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
        
    # Q23 : Recommander un film à un acteur
    if st.button("23 - Recommander un film à un acteur"):
        actor_name = st.text_input("Nom de l'acteur", value="Anne Hathaway")
        if actor_name:
            manager = Neo4jManager()
            recommendation = manager.recommander_film_a_acteur(actor_name)
            if recommendation:
                st.success(f"Film recommandé pour {actor_name} : {recommendation['film']} ({recommendation['year']}), Votes: {recommendation['votes']}")
            else:
                st.warning("Aucune recommandation trouvée")
            manager.close()

    # Q24 : Créer relation INFLUENCE_PAR entre réalisateurs
    if st.button("24 - Créer relation INFLUENCE_PAR entre réalisateurs"):
        manager = Neo4jManager()
        relations = manager.creer_relation_influence()
        if relations:
            for r in relations:
                st.write(f"{r['director1']} influence {r['director2']} (poids : {r['influence']})")
        else:
            st.info("Aucune relation créée (vérifiez le seuil ou les données)")
        manager.close()

    # Q25 : Chemin le plus court entre deux acteurs
    if st.button("25 - Chemin le plus court entre deux acteurs"):
        acteur1 = st.text_input("Nom du premier acteur", value="Tom Hanks")
        acteur2 = st.text_input("Nom du second acteur", value="Scarlett Johansson")
        if acteur1 and acteur2:
            manager = Neo4jManager()
            path_record = manager.chemin_plus_court(acteur1, acteur2)
            if path_record and "p" in path_record:
                st.write("Chemin trouvé :", path_record["p"])
            else:
                st.warning("Aucun chemin trouvé")
            manager.close()

    # Q26 : (La section Q26 est actuellement commentée)
    # if st.button("26 - Analyse des communautés d'acteurs (Louvain)"):
    #     manager = Neo4jManager()
    #     communities = manager.communautes_acteurs()
    #     if communities:
    #         st.dataframe(pd.DataFrame(communities))
    #     else:
    #         st.warning("Aucune communauté détectée")
    #     manager.close()
        
    # Q27 : Films avec genres en commun et réalisateurs différents
    if st.button("27 - Films avec genres en commun et réalisateurs différents"):
        manager = Neo4jManager()
        results = manager.films_genres_communs_differents_realisateurs()
        if results:
            for rec in results:
                st.write(f"{rec['film1']} et {rec['film2']} partagent le genre '{rec['genre']}'")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
        
    # Q28 : Recommander des films en fonction d'un acteur
    if st.button("28 - Recommander des films en fonction d'un acteur"):
        actor_name = st.text_input("Nom de l'acteur", value="Anne Hathaway")
        if actor_name:
            manager = Neo4jManager()
            recommendations = manager.recommander_films_utilisateur(actor_name)
            if recommendations:
                for rec in recommendations:
                    st.write(f"{rec['film']} ({rec['year']}), Votes: {rec['votes']}")
            else:
                st.warning("Aucune recommandation trouvée")
            manager.close()
            
    # Q29 : Créer relation CONCURRENCE entre réalisateurs
    if st.button("29 - Créer relation CONCURRENCE entre réalisateurs"):
        manager = Neo4jManager()
        relations = manager.creer_relation_concurrence()
        if relations:
            for r in relations:
                st.write(f"{r['director1']} et {r['director2']} (Année: {r['year']}, Genres en commun: {r['commonGenres']})")
        else:
            st.warning("Aucune relation créée")
        manager.close()
        
    # Q30 : Collaborations fréquentes entre réalisateurs et acteurs
    if st.button("30 - Collaborations fréquentes entre réalisateurs et acteurs"):
        manager = Neo4jManager()
        collabs = manager.collaborations_realisateurs_acteurs()
        if collabs:
            st.dataframe(pd.DataFrame(collabs))
        else:
            st.warning("Aucune collaboration trouvée")
        manager.close()


# =============================================================================
# Fonction principale : point d'entrée de l'application Streamlit
# =============================================================================
def main():
    st.set_page_config(page_title="Projet NoSQL", layout="wide")
    st.title("🎬 Projet NoSQL - Brayane MBA ABESSOLO")
    st.markdown("""
    **Objectif** : Explorer une base de données MongoDB/Neo4j contenant des informations sur des films
    """)
    
    # Navigation via la barre latérale pour sélectionner le module d'analyse
    page = st.sidebar.radio(
        "Menu principal",
        ["MongoDB - Analyse de films", "Neo4j - Relations", "Visualisations avancées"],
        index=0
    )
    if page == "MongoDB - Analyse de films":
        from requete.requete_mongo import requete_mongo
        requete_mongo()
    elif page == "Neo4j - Relations":
        from requete.requete_neo4j import requete_neo4j
        requete_neo4j()  # Appel du module des requêtes Neo4j
    else:
        st.warning("Module de visualisation en cours de développement...")


# =============================================================================
# Point d'entrée de l'application
# =============================================================================
if __name__ == "__main__":
    main()
