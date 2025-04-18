# =============================================================================
# Fichier : app.py
# Description : Application Streamlit pour interroger et visualiser des données 
#               issues de MongoDB et Neo4j pour l'analyse de films.
# =============================================================================

# Importation des modules nécessaires pour Streamlit, le traitement des données 
# et la visualisation
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Importation des fonctions de requêtes pour MongoDB depuis le module requete
from requete.requete_mongo import (
    annee_avec_plus_de_film,
    films_apres_1999,
    moyenne_votes_2007,
    histogramme_films_par_annee,
    genres_disponibles,
    film_plus_gros_revenu,
    realisateurs_plus_de_5_films,
    genre_plus_rentable,
    get_genre_revenues,
    top_films_par_decennie,
    film_plus_long_par_genre,
    creer_vue_films_premium,
    correlation_duree_revenu,
    duree_moyenne_par_decennie,
    exporter_donnees_pour_neo4j
)

# Importation du module pour l'importation des données vers Neo4j
from import_neo4j import Neo4jImporter


# =============================================================================
# Fonction get_db : retourne l'objet de base de données MongoDB connecté.
# =============================================================================
@st.cache_resource
def get_db():
    """Retourne l'objet 'db' déjà connecté à la base MongoDB 'entertainment'."""
    client = MongoClient("mongodb+srv://BrayaneLaugane2001:BrayaneLaugane2001@cluster.aqxgh9e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster")
    return client.entertainment


# =============================================================================
# Fonction show_genre_revenue_comparison : affiche un graphique comparatif des revenus
# par genre en utilisant les données récupérées depuis MongoDB.
# =============================================================================
def show_genre_revenue_comparison():
    """Affiche un graphique comparatif des revenus moyens par genre."""
    # Importation de la fonction get_genre_revenues pour récupérer les données
    from requete.requete_mongo import get_genre_revenues
    genre_data = get_genre_revenues()
    st.write("Données reçues:", genre_data)
    
    if genre_data:
        df = pd.DataFrame(genre_data)
        # Création du graphique sous forme de barres
        fig, ax = plt.subplots(figsize=(10, 6))
        df.plot.bar(x='Genre', y='Revenu moyen', ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)


# =============================================================================
# Fonction requete_mongo : affiche l'ensemble des requêtes liées à MongoDB dans 
# l'interface Streamlit.
# =============================================================================
def requete_mongo():
    st.header("📊 Requêtes MongoDB")
    
    # Requête 1 : Année avec le plus de films
    if st.button("1 - Année avec le plus de films"):
        result = annee_avec_plus_de_film()
        st.success(f"Année record : {result['Année']} ({result['Nombre de films']} films)")
    
    # Requête 2 : Nombre de films après 1999
    if st.button("2 - Nombre de films après 1999"):
        result = films_apres_1999()
        st.success(f"Nombre de films après 1999 : {result['Nombre de films après 1999']}")
    
    # Requête 3 : Moyenne des votes en 2007
    if st.button("3 - Moyenne des votes en 2007"):
        result = moyenne_votes_2007()
        moyenne = result["Moyenne des votes en 2007"]
        if moyenne != "N/A":
            st.success(f"📊 Moyenne des votes en 2007 : {moyenne}")
        else:
            st.warning("⚠️ Aucune donnée de votes disponible pour l'année 2007.")
    
    # Requête 4 : Histogramme des films par année
    if st.button("4 - Histogramme des films par année"):
        buf = histogramme_films_par_annee()
        if buf:
            st.image(buf)
            st.download_button(
                "Télécharger l'histogramme",
                data=buf,
                file_name="films_par_annee.png",
                mime="image/png"
            )
    
    # Requête 5 : Liste des genres disponibles
    if st.button("5 - Lister tous les genres"):
        result = genres_disponibles()
        if "Genres uniques" in result:
            st.subheader("Genres disponibles")
            col1, col2, col3 = st.columns(3)
            genres = result["Genres uniques"]
            tiers = len(genres) // 3
            with col1:
                for genre in genres[:tiers]:
                    st.markdown(f"• {genre}")
            with col2:
                for genre in genres[tiers:2*tiers]:
                    st.markdown(f"• {genre}")
            with col3:
                for genre in genres[2*tiers:]:
                    st.markdown(f"• {genre}")
            st.markdown("---")
            st.success(f"**Total de genres uniques :** {len(genres)}")
        elif "Erreur" in result:
            st.error(result["Erreur"])
            
    # Requête 6 : Film avec le plus gros revenu
    if st.button("6 - Film avec le plus gros revenu"):
        result = film_plus_gros_revenu()
        if "Erreur" not in result:
            revenu_text = (
                f"💰 Revenu généré : **{result['Revenu formaté']}**"
                if result["Revenu"] is not None
                else "💰 Revenu non disponible"
            )
            st.success(
                f"Film le plus rentable : **{result['Titre']}** ({result['Année']})\n{revenu_text}"
            )
            if result["Revenu"] is not None:
                st.info(f"Valeur exacte : {result['Revenu']:,.2f} M$")
        else:
            st.error(result["Erreur"])
            
    # Requête 7 : Réalisateurs avec plus de 5 films
    if st.button("7 - Réalisateurs avec plus de 5 films"):
        results = realisateurs_plus_de_5_films()
        if results:
            st.success("Réalisateurs ayant plus de 5 films:")
            # Affichage des résultats sous forme de tableau
            df = pd.DataFrame(results)
            st.dataframe(df)
        else:
            st.warning("Aucun réalisateur n'a plus de 5 films dans la base")
    
    # Requête 8 : Genre le plus rentable en moyenne
    if st.button("8 - Genre le plus rentable (moyenne)"):
        result = genre_plus_rentable()
        if "Erreur" not in result:
            st.success(
                f"Genre: {result['Genre']}\nRevenu moyen: {result['Revenu moyen']} $"
            )
            show_genre_revenue_comparison()
        else:
            st.error(result["Erreur"])
    
    # Requête 9 : Top 3 films par décennie
    if st.button("9 - Top 3 films par décennie (depuis 1990)"):
        st.markdown("**Critères :** Films 'G' prioritaires → Tri par nombre de votes (↓)")
        decennies = top_films_par_decennie()
        if not decennies:
            st.warning("Aucun film trouvé après 1990")
            return
        for groupe in decennies:
            with st.expander(f"📅 Décennie {groupe['Decennie']}"):
                for i, film in enumerate(groupe["Top 3"], 1):
                    st.markdown(f"""
                    **{i}. {film['Titre']}** ({film['Année']})  
                    🏷️ Rating: `{film['Rating']}`  
                    👍 Votes: `{film['Votes']:,}`  
                    🎬 Réalisateur: *{film['Réalisateur']}*
                    """)
                    st.divider()
    
    # Requête 10 : Film le plus long par genre
    if st.button("10 - Film le plus long par genre"):
        results = film_plus_long_par_genre()
        if not results:
            st.warning("Aucun résultat trouvé")
            return
        df = pd.DataFrame(results)
        st.dataframe(
            df.style.highlight_max(subset=["Durée (min)"], color="#90EE90"),
            column_config={
                "Durée (min)": st.column_config.NumberColumn("Durée (min)", help="Durée en minutes"),
                "Votes": st.column_config.NumberColumn("Votes", format="%,d")
            }
        )
    
    # Requête 11 : Création et affichage de la vue "films_premium"
    if st.button("11 - Films premium (Metascore > 80 & Revenue > 50M)"):
        db = get_db()  # Connexion à la base MongoDB
        try:
            pipeline = [
                {"$match": {"Metascore": {"$gt": 80}, "Revenue (Millions)": {"$gt": 50}}},
                {"$project": {"_id": 1, "title": 1, "year": 1, "Director": 1}}
            ]
            db.command("create", "films_premium", viewOn="films", pipeline=pipeline)
            films = list(db.films_premium.find().sort("Metascore", -1))
            st.dataframe(pd.DataFrame(films))
        except Exception as e:
            if "already exists" in str(e):
                st.info("Voici le résultat :")
                films = list(db.films_premium.find().sort("Metascore", -1))
                st.dataframe(pd.DataFrame(films))
            else:
                st.error(f"Erreur : {e}")
    
    # Requête 12 : Corrélation Durée-Revenu
    if st.button("12 - Corrélation Durée-Revenu"):
        result = correlation_duree_revenu()
        if "error" in result:
            st.error(result["error"])
        else:
            st.markdown("### 📊 Analyse Statistique")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Coefficient de corrélation", f"{result['correlation']:.2f}")
            with col2:
                st.metric("Films analysés", result["sample_size"])
            st.image(result["plot_path"])
            st.markdown("""
            **Interprétation :**  
            - **> 0.7** : Forte corrélation positive  
            - **0.3 - 0.7** : Corrélation modérée  
            - **< 0.3** : Faible corrélation  
            """)
    
    # Requête 13 : Durée moyenne des films par décennie
    if st.button("13 - Durée moyenne des films par décennie"):
        results = duree_moyenne_par_decennie()
        if not results:
            st.warning("Aucune donnée disponible.")
        else:
            df = pd.DataFrame(list(results.items()), columns=["Décennie", "Durée moyenne (min)"])
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(df["Décennie"], df["Durée moyenne (min)"], marker="o", linestyle="-")
            ax.set_title("Évolution de la durée moyenne des films par décennie")
            ax.set_xlabel("Décennie")
            ax.set_ylabel("Durée moyenne (minutes)")
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)
            st.dataframe(df.style.highlight_max(axis=0, color="#90EE90"))
    
    # -------------------------------------------------------------------------
    # Navigation entre les modules d'analyse
    # -------------------------------------------------------------------------
    st.header("Navigation et modules complémentaires")
    # Ici, d'autres modules peuvent être ajoutés, par exemple pour les requêtes Neo4j.
    

# =============================================================================
# Fonction main : point d'entrée de l'application
# =============================================================================
def main():
    st.set_page_config(page_title="Projet NoSQL", layout="wide")
    st.title("🎬 Projet NoSQL - Brayane MBA ABESSOLO")
    st.markdown("""
    **Objectif** : Explorer une base de données MongoDB/Neo4j contenant des informations sur des films
    """)
    
    # Sélection du module via la barre latérale
    page = st.sidebar.radio(
        "Menu principal",
        ["MongoDB - Analyse de films", "Neo4j - Relations"],
        index=0
    )
    if page == "MongoDB - Analyse de films":
        requete_mongo()
    if page == "Neo4j - Relations":
        from requete.requete_neo4j import requete_neo4j
        requete_neo4j()  # Appel du module Neo4j
    


# =============================================================================
# Point d'entrée de l'application
# =============================================================================
if __name__ == "__main__":
    main()
