# Import des modules nécessaires pour la connexion à MongoDB, le traitement de données et la visualisation
from database.connexion_mongo import connexion_mongo
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import seaborn as sns
import io

# Établissement de la connexion MongoDB et accès à la collection 'films'
client = connexion_mongo()
db = client.entertainment
films = db.films


# ----------------------------
# Requête 1 : Année avec le plus grand nombre de films
# ----------------------------
def annee_avec_plus_de_film():
    """
    Retourne l'année qui compte le plus grand nombre de films et le nombre de films correspondants.
    """
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(films.aggregate(pipeline))
    if result:
        return {"Année": result[0]["_id"], "Nombre de films": result[0]["count"]}
    return {"Année": "N/A", "Nombre de films": 0}


# ----------------------------
# Requête 2 : Nombre de films sortis après 1999
# ----------------------------
def films_apres_1999():
    """
    Retourne le nombre de films sortis après l'année 1999.
    """
    count = films.count_documents({"year": {"$gt": 1999}})
    return {"Nombre de films après 1999": count}


# ----------------------------
# Requête 3 : Moyenne des votes des films sortis en 2007
# ----------------------------
def moyenne_votes_2007():
    """
    Calcule et retourne la moyenne des votes pour les films de l'année 2007.
    """
    pipeline = [
        {"$match": {"year": 2007, "votes": {"$exists": True, "$ne": None}}},
        {"$group": {"_id": None, "avg_votes": {"$avg": "$votes"}}}
    ]
    result = list(films.aggregate(pipeline))
    # Vérification renforcée pour s'assurer que la moyenne est bien calculée
    if result and "avg_votes" in result[0] and isinstance(result[0]["avg_votes"], (int, float)):
        return {"Moyenne des votes en 2007": round(result[0]["avg_votes"], 2)}
    return {"Moyenne des votes en 2007": "N/A"}


# ----------------------------
# Requête 4 : Histogramme du nombre de films par année
# ----------------------------
def histogramme_films_par_annee():
    """
    Crée un histogramme du nombre de films par année et retourne le graphique sous forme de buffer.
    """
    pipeline = [{"$match": {"year": {"$exists": True}}},
                {"$group": {"_id": "$year", "count": {"$sum": 1}}}]
    data = list(films.aggregate(pipeline))
    df = pd.DataFrame(data)
    df = df.dropna()
    
    # Création du graphique
    plt.figure(figsize=(12, 6))
    plt.bar(df["_id"].astype(int), df["count"])
    plt.xlabel("Année")
    plt.ylabel("Nombre de films")
    plt.title("Nombre de films par année")
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Sauvegarde du graphique dans un buffer en mémoire
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close()
    buf.seek(0)
    return buf


# ----------------------------
# Requête 5 : Extraction des genres disponibles
# ----------------------------
def genres_disponibles():
    """
    Retourne une liste triée des genres uniques présents dans la collection des films.
    """
    try:
        # Récupère toutes les entrées distinctes de genre
        genres_distincts = films.distinct("genre")
        
        # Sépare et nettoie les genres pour éliminer les doublons
        genres_uniques = set()
        for genre_str in genres_distincts:
            if genre_str:  # Ignore les valeurs vides
                for genre in genre_str.split(','):
                    genre_nettoye = genre.strip()
                    if genre_nettoye:
                        genres_uniques.add(genre_nettoye)
        
        # Retourne les genres triés
        return {"Genres uniques": sorted(genres_uniques)}
    
    except Exception as e:
        return {"Erreur": f"Erreur lors de la récupération des genres: {str(e)}"}


# ----------------------------
# Requête 6 : Film avec le plus gros revenu
# ----------------------------
def film_plus_gros_revenu():
    """
    Retourne les informations du film qui a généré le plus gros revenu.
    """
    try:
        pipeline = [
            {"$sort": {"Revenue (Millions)": -1}},
            {"$limit": 1},
            {"$project": {
                "Titre": "$title",
                "Année": "$year",
                "Revenu": "$Revenue (Millions)",
                "Director": 1,
                "Actors": 1
            }}
        ]
        result = list(db.films.aggregate(pipeline))
        
        if not result:
            return {"Erreur": "Aucun film trouvé"}
        
        film = result[0]
        revenu = film.get("Revenu")
        
        # Conversion sécurisée du revenu en nombre
        try:
            revenu_num = float(revenu) if revenu is not None else None
        except (ValueError, TypeError):
            revenu_num = None
        
        return {
            "Titre": film["Titre"],
            "Année": film["Année"],
            "Revenu": revenu_num
        }
        
    except Exception as e:
        return {"Erreur": f"Erreur MongoDB: {str(e)}"}


# ----------------------------
# Requête 7 : Réalisateurs ayant réalisé plus de 5 films
# ----------------------------
def realisateurs_plus_de_5_films():
    """
    Retourne une liste de dictionnaires contenant le nom du réalisateur et le nombre de films,
    pour les réalisateurs ayant réalisé plus de 5 films.
    """
    pipeline = [
        {"$group": {
            "_id": "$Director",
            "count": {"$sum": 1}
        }},
        {"$match": {
            "count": {"$gt": 5}
        }},
        {"$sort": {
            "count": -1
        }},
        {"$project": {
            "_id": 0,
            "Realisateur": "$_id",
            "Nombre de films": "$count"
        }}
    ]
    results = list(films.aggregate(pipeline))
    return results if results else []


# ----------------------------
# Requête 8 : Genre de film le plus rentable en moyenne
# ----------------------------
def genre_plus_rentable():
    """
    Retourne le genre dont le revenu moyen est le plus élevé.
    """
    pipeline = [
        {"$unwind": "$Genre"},  # Dédouble les films ayant plusieurs genres
        {"$group": {
            "_id": "$Genre",
            "revenu_moyen": {"$avg": "$Revenue"}
        }},
        {"$sort": {"revenu_moyen": -1}},
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "Genre": "$_id",
            "Revenu moyen": {"$round": ["$revenu_moyen", 2]}
        }}
    ]
    result = list(films.aggregate(pipeline))
    return result[0] if result else {"Erreur": "Aucun résultat trouvé"}


def get_genre_revenues():
    """
    Récupère les revenus moyens pour tous les genres, utile pour des visualisations.
    """
    pipeline = [
        {"$unwind": "$Genre"},
        {"$group": {
            "_id": "$Genre",
            "Revenu moyen": {"$avg": "$Revenue"}
        }},
        {"$sort": {"Revenu moyen": -1}},
        {"$project": {
            "_id": 0,
            "Genre": "$_id",
            "Revenu moyen": {"$round": ["$Revenu moyen", 2]}
        }}
    ]
    return list(films.aggregate(pipeline))


# ----------------------------
# Requête 9 : Top 3 films par décennie depuis 1990
# ----------------------------
def top_films_par_decennie():
    """
    Retourne les 3 meilleurs films par décennie depuis 1990.
    Critères de classement :
      1. Priorité aux films avec rating "G" sur "unrated".
      2. En cas d'égalité, tri par nombre de votes décroissant.
    """
    pipeline = [
        {"$match": {
            "year": {"$gte": 1990, "$exists": True},
            "rating": {"$in": ["G", "unrated"]},
            "Votes": {"$exists": True, "$ne": None}
        }},
        {"$addFields": {
            "Decennie": {
                "$concat": [
                    {"$toString": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}},
                    "-",
                    {"$toString": {"$add": [{"$subtract": ["$year", {"$mod": ["$year", 10]}]}, 9]}}
                ]
            },
            "RatingPriority": {
                "$cond": [{"$eq": ["$rating", "G"]}, 1, 2]
            }
        }},
        {"$sort": {
            "Decennie": 1,
            "RatingPriority": 1,
            "Votes": -1
        }},
        {"$group": {
            "_id": "$Decennie",
            "films": {
                "$push": {
                    "Titre": "$title",
                    "Année": "$year",
                    "Rating": "$rating",
                    "Votes": "$Votes",
                    "Réalisateur": "$Director"
                }
            }
        }},
        {"$project": {
            "Decennie": "$_id",
            "Top 3": {"$slice": ["$films", 3]},
            "_id": 0
        }}
    ]
    return list(films.aggregate(pipeline))


# ----------------------------
# Requête 10 : Film le plus long par genre
# ----------------------------
def film_plus_long_par_genre():
    """
    Retourne le film le plus long (en minutes) pour chaque genre.
    En cas d'égalité sur la durée, le film avec le plus de votes est retenu.
    """
    pipeline = [
        {"$match": {
            "Runtime (Minutes)": {"$exists": True, "$ne": None},
            "genre": {"$exists": True, "$ne": None}
        }},
        {"$addFields": {
            "Genres": {
                "$split": ["$genre", ","]
            }
        }},
        {"$unwind": "$Genres"},
        {"$addFields": {
            "Genres": {"$trim": {"input": "$Genres"}}
        }},
        {"$sort": {
            "Genres": 1,
            "Runtime (Minutes)": -1,
            "Votes": -1
        }},
        {"$group": {
            "_id": "$Genres",
            "Titre": {"$first": "$title"},
            "Runtime": {"$first": "$Runtime (Minutes)"},
            "Année": {"$first": "$year"},
            "Réalisateur": {"$first": "$Director"},
            "Votes": {"$first": "$Votes"}
        }},
        {"$project": {
            "_id": 0,
            "Genre": "$_id",
            "Titre": 1,
            "Durée (min)": "$Runtime",
            "Année": 1,
            "Réalisateur": 1,
            "Votes": 1
        }},
        {"$sort": {"Genre": 1}}
    ]
    return list(films.aggregate(pipeline))


# ----------------------------
# Requête 11 : Création d'une vue MongoDB pour les films premium
# ----------------------------
def creer_vue_films_premium(db):
    """
    Crée une vue 'films_premium' dans MongoDB pour afficher uniquement les films ayant 
    un Metascore supérieur à 80 et un revenu supérieur à 50 millions.
    """
    view_name = "films_premium"
    pipeline = [
        {"$match": {"Metascore": {"$gt": 80, "$exists": True}, "Revenue (Millions)": {"$gt": 50, "$exists": True}}},
        {"$project": {"_id": 1, "title": 1, "year": 1, "Metascore": 1, "Revenue (Millions)": 1, "Director": 1, "rating": 1}}
    ]
    
    if view_name not in db.list_collection_names():
        db.command({
            "create": view_name,
            "viewOn": "films", 
            "pipeline": pipeline
        })
        return True
    return False


# ----------------------------
# Requête 12 : Corrélation entre la durée et le revenu des films
# ----------------------------
def correlation_duree_revenu():
    """
    Calcule la corrélation entre la durée (Runtime) et le revenu (Revenue) des films.
    Gère différents formats pour la propriété 'Revenue (Millions)' et retourne un graphique.
    """
    pipeline = [
        {"$match": {
            "Runtime (Minutes)": {"$exists": True, "$ne": None, "$type": "number"},
            "Revenue (Millions)": {"$exists": True, "$ne": None, "$ne": ""}
        }},
        {"$addFields": {
            "RevenueClean": {
                "$cond": [
                    {"$eq": [{"$type": "$Revenue (Millions)"}, "string"]},
                    {"$toDouble": {"$replaceOne": {"input": "$Revenue (Millions)", "find": ",", "replacement": "."}}},
                    "$Revenue (Millions)"
                ]
            }
        }},
        {"$match": {
            "RevenueClean": {"$type": "number"}
        }},
        {"$project": {
            "Titre": "$title",
            "Runtime": "$Runtime (Minutes)",
            "Revenue": "$RevenueClean"
        }}
    ]
    data = list(films.aggregate(pipeline))
    df = pd.DataFrame(data)
    
    if df.empty:
        return {"error": "Aucune donnée valide après nettoyage."}
    
    # Calcul de la corrélation
    corr = df["Runtime"].corr(df["Revenue"])
    
    # Création du graphique
    plt.figure(figsize=(10, 6))
    sns.regplot(x="Runtime", y="Revenue", data=df, scatter_kws={"alpha": 0.5})
    plt.title("Corrélation Durée vs Revenu (en millions $)")
    plt.xlabel("Durée (minutes)")
    plt.ylabel("Revenu (Millions $)")
    plot_path = "correlation_duree_revenu.png"
    plt.savefig(plot_path)
    plt.close()
    
    return {
        "correlation": corr,
        "plot_path": plot_path,
        "sample_size": len(df),
        "excluded_count": films.count_documents({}) - len(df)
    }


# ----------------------------
# Requête 13 : Durée moyenne des films par décennie
# ----------------------------
def duree_moyenne_par_decennie():
    """
    Calcule la durée moyenne des films par décennie et retourne un dictionnaire 
    avec la décennie et la durée moyenne arrondie à une décimale.
    """
    pipeline = [
        {"$match": {
            "year": {"$gte": 1990, "$exists": True},
            "Runtime (Minutes)": {"$exists": True, "$ne": None, "$type": "number"}
        }},
        {"$addFields": {
            "decennie": {
                "$concat": [
                    {"$toString": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}},
                    "-",
                    {"$toString": {"$add": [{"$subtract": ["$year", {"$mod": ["$year", 10]}]}, 9]}}
                ]
            }
        }},
        {"$group": {
            "_id": "$decennie",
            "duree_moyenne": {"$avg": "$Runtime (Minutes)"},
            "count": {"$sum": 1}
        }},
        {"$project": {
            "decennie": "$_id",
            "duree_moyenne": {"$round": ["$duree_moyenne", 1]},
            "count": 1,
            "_id": 0
        }},
        {"$sort": {"decennie": 1}}
    ]
    
    results = list(films.aggregate(pipeline))
    return {res["decennie"]: res["duree_moyenne"] for res in results}


# ----------------------------
# Exportation des données de MongoDB vers Neo4j
# ----------------------------
def exporter_donnees_pour_neo4j():
    """
    Exporte les données nécessaires pour l'importation dans Neo4j.
    Retourne une liste de documents contenant les champs essentiels.
    """
    client = connexion_mongo()
    db = client.entertainment
    films = db.films.find({}, {
        "_id": 1,
        "title": 1,
        "year": 1,
        "Votes": 1,
        "Revenue (Millions)": 1,
        "rating": 1,
        "Director": 1,
        "Actors": 1,
        "genre": 1
    })
    return list(films)
