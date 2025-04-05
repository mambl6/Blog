from database.connexion_neo4j import connexion_neo4j
from import_neo4j import Neo4jImporter
import streamlit as st
from neo4j import GraphDatabase
import pandas as pd


class Neo4jManager:
    def __init__(self):
        self.driver = connexion_neo4j()
        
    def close(self):
        if hasattr(self, 'driver'):
            self.driver.close()
    
    
    ## Méthodes Q14
    
      
    
    ## Méthodes Q15        
    def acteurs_avec_anne_hathaway(self):
        """Question 15 - Acteurs ayant joué avec Anne Hathaway"""
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

    ## Méthodes Q16
    def acteur_plus_rentable(self):
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

    ## Méthodes Q17
    def moyenne_votes(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (f:Film)
                WHERE f.votes IS NOT NULL
                RETURN round(avg(toInteger(f.votes))) AS moyenne_votes
            """)
            record = result.single()
            return record["moyenne_votes"] if record else None

    ## Méthodes Q18
    def genre_plus_represente(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (f:Film)-[:A_POUR_GENRE]->(g:Genre)
                RETURN g.name AS genre, COUNT(f) AS nb_films
                ORDER BY nb_films DESC
                LIMIT 1
            """)
            record = result.single()
            return dict(record) if record else None

    ## Méthodes Q19
    def films_coacteurs(self):
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
        
    ## Méthodes Q20 
    def realisateur_plus_acteurs_distincts(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (d:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Acteur)
                RETURN d.name AS director, count(DISTINCT a.name) AS nb_acteurs
                ORDER BY nb_acteurs DESC
                LIMIT 1
            """)
            record = result.single()
            return dict(record) if record else None
    
    ## Méthodes Q21
    def films_plus_connectes(self):
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
    
    ## Méthodes Q22
    def top_5_acteurs_realisateurs(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Acteur)-[:A_JOUE_DANS]->(f:Film)<-[:A_REALISE]-(d:Realisateur)
                RETURN a.name AS actor, count(DISTINCT d.name) AS nb_directeurs
                ORDER BY nb_directeurs DESC
                LIMIT 5
            """)
            return [dict(record) for record in result]
    
    ## Méthodes Q23
    def recommander_film_a_acteur(self, actorName):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Acteur {name: $actorName})-[:A_JOUE_DANS]->(f:Film)-[:A_POUR_GENRE]->(g:Genre)
                WITH a, collect(DISTINCT g) AS genres
                MATCH (rec:Film)-[:A_POUR_GENRE]->(g)
                WHERE g IN genres AND NOT (a)-[:A_JOUE_DANS]->(rec)
                RETURN rec.title AS film, rec.year AS year, rec.votes AS votes
                ORDER BY rec.votes DESC
                LIMIT 1
            """, {'actorName': actorName})
            record = result.single()
            return dict(record) if record else None


    ## Méthodes Q24
    def creer_relation_influence(self, seuil=2):
        """
        Crée des relations INFLUENCE_PAR entre les réalisateurs 
        si le nombre de genres communs (entre leurs films) est supérieur ou égal au seuil.
        Le paramètre 'seuil' est ajustable (défaut = 2).
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
            # On retourne la liste des relations créées
            return [dict(record) for record in result]
        
    ## Méthodes Q25
    def chemin_plus_court(self, acteur1, acteur2):
        """
        Trouve le chemin le plus court entre deux acteurs.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH p = shortestPath((a:Acteur {name: $acteur1})-[*]-(b:Acteur {name: $acteur2}))
                RETURN p
            """, {'acteur1': acteur1, 'acteur2': acteur2})
            return result.single()

    ## Méthodes Q26
    def communautes_acteurs(self):
        with self.driver.session() as session:
            result = session.run("""
                CALL gds.louvain.stream("{\\"nodeProjection\\": \\"Acteur\\", \\"relationshipProjection\\": \\"A_JOUE_DANS\\"}")
                YIELD nodeId, communityId
                RETURN gds.util.asNode(nodeId).name AS actor, communityId
                ORDER BY communityId, actor
            """)
            return [dict(record) for record in result]
        
        
    ## Méthodes Q27
    def films_genres_communs_differents_realisateurs(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (f1:Film)-[:A_POUR_GENRE]->(g:Genre)<-[:A_POUR_GENRE]-(f2:Film)
                WHERE f1 <> f2
                  AND f1.year = f2.year  // facultatif si vous souhaitez comparer pour la même année
                  AND NOT EXISTS {
                      MATCH (f1)<-[:A_REALISE]-(d1:Realisateur),
                            (f2)<-[:A_REALISE]-(d2:Realisateur)
                      WHERE d1 = d2
                  }
                RETURN f1.title AS film1, f2.title AS film2, g.name AS genre
                ORDER BY film1, film2
            """)
            return [dict(record) for record in result]
        
    ## Méthodes Q28
    def recommander_films_utilisateur(self, actorName):
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
        
    ## Méthodes Q29
    def creer_relation_concurrence(self):
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
        
    ## Méthodes Q30
    def collaborations_realisateurs_acteurs(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (d:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Acteur)
                WITH d, a, count(f) AS nb_collaborations, avg(f.revenue) AS avg_revenue, avg(f.Metascore) AS avg_metascore
                RETURN d.name AS director, a.name AS actor, nb_collaborations, avg_revenue, avg_metascore
                ORDER BY nb_collaborations DESC
                LIMIT 10
            """)
            return [dict(record) for record in result]


 

def requete_neo4j():
    st.header("🔄 Requêtes Neo4j")
    
    # Section Import
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

    # Section Requêtes
    st.header("📊 Requêtes Neo4j")
    
    
    ##### à partir d'ici vous sverrez les boutons Streamlit qui appellent ces méthodes
    
    # Question 14
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
            
    # Question 15        
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
                    
    # Question 16
    if st.button("16 - Acteur avec le plus gros revenu total"):
        manager = Neo4jManager()
        result = manager.acteur_plus_rentable()
        if result:
            st.success(f"💰 {result['acteur']} - Revenu total: ${result['revenu_total']:,.2f}M ({result['nb_films']} films)")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()

    # Question 17
    if st.button("17 - Moyenne des votes"):
        manager = Neo4jManager()
        moyenne = manager.moyenne_votes()
        st.success(f"⭐ Moyenne des votes: {moyenne:,.0f}")
        manager.close()

    # Question 18
    if st.button("18 - Genre le plus représenté"):
        manager = Neo4jManager()
        result = manager.genre_plus_represente()
        if result:
            st.success(f"🎬 Genre dominant: {result['genre']} ({result['nb_films']} films)")
        else:
            st.warning("Aucun genre trouvé")
        manager.close()

    # Question 19
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
    
    # Question 20
    if st.button("20 - Réalisateur avec le plus d'acteurs distincts"):
        manager = Neo4jManager()
        result = manager.realisateur_plus_acteurs_distincts()
        if result:
            st.success(f"🎬 {result['director']} a travaillé avec {result['nb_acteurs']} acteurs distincts")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
    
    # Question 21
    if st.button("21 - Films les plus connectés"):
        manager = Neo4jManager()
        results = manager.films_plus_connectes()
        if results:
            for res in results:
                st.write(f"Film : {res['film']} - Acteurs communs : {res['commonActors']}")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
        
    # Question 22
    if st.button("22 - Top 5 acteurs avec le plus de réalisateurs différents"):
        manager = Neo4jManager()
        results = manager.top_5_acteurs_realisateurs()
        if results:
            for res in results:
                st.write(f"Acteur : {res['actor']} - Nombre de réalisateurs : {res['nb_directeurs']}")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
        
    # Question 23
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

    # Question 24
    if st.button("24 - Créer relation INFLUENCE_PAR entre réalisateurs"):
        manager = Neo4jManager()
        relations = manager.creer_relation_influence()
        if relations:
            for r in relations:
                st.write(f"{r['director1']} influence {r['director2']} (poids : {r['influence']})")
        else:
            st.info("Aucune relation créée (vérifiez le seuil ou les données)")
        manager.close()

    # Question 25
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

    # Question 26
    if st.button("26 - Analyse des communautés d'acteurs (Louvain)"):
        manager = Neo4jManager()
        communities = manager.communautes_acteurs()
        if communities:
            st.dataframe(pd.DataFrame(communities))
        else:
            st.warning("Aucune communauté détectée")
        manager.close()
        
    # Question 27
    if st.button("27 - Films avec genres en commun et réalisateurs différents"):
        manager = Neo4jManager()
        results = manager.films_genres_communs_differents_realisateurs()
        if results:
            for rec in results:
                st.write(f"{rec['film1']} et {rec['film2']} partagent le genre '{rec['genre']}'")
        else:
            st.warning("Aucun résultat trouvé")
        manager.close()
        
    # Question 28
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
            
    # Question 29
    if st.button("29 - Créer relation CONCURRENCE entre réalisateurs"):
        manager = Neo4jManager()
        relations = manager.creer_relation_concurrence()
        if relations:
            for r in relations:
                st.write(f"{r['director1']} et {r['director2']} (Année: {r['year']}, Genres en commun: {r['commonGenres']})")
        else:
            st.warning("Aucune relation créée")
        manager.close()
        
    # Question 30
    if st.button("30 - Collaborations fréquentes entre réalisateurs et acteurs"):
        manager = Neo4jManager()
        collabs = manager.collaborations_realisateurs_acteurs()
        if collabs:
            st.dataframe(pd.DataFrame(collabs))
        else:
            st.warning("Aucune collaboration trouvée")
        manager.close()
    

    
    

