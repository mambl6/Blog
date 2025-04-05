from database.connexion_mongo import connexion_mongo
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import seaborn as sns
import io

client = connexion_mongo()
db = client.entertainment
films = db.films

def annee_avec_plus_de_film():
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(films.aggregate(pipeline))
    if result:
        return {"Année": result[0]["_id"], "Nombre de films": result[0]["count"]}
    return {"Année": "N/A", "Nombre de films": 0}

def films_apres_1999():
    count = films.count_documents({"year": {"$gt": 1999}})
    return {"Nombre de films après 1999": count}

def moyenne_votes_2007():
    pipeline = [
        {"$match": {"year": 2007, "votes": {"$exists": True, "$ne": None}}},
        {"$group": {"_id": None, "avg_votes": {"$avg": "$votes"}}}
    ]
    result = list(films.aggregate(pipeline))

    # Vérification renforcée
    if result and "avg_votes" in result[0] and isinstance(result[0]["avg_votes"], (int, float)):
        return {"Moyenne des votes en 2007": round(result[0]["avg_votes"], 2)}

    return {"Moyenne des votes en 2007": "N/A"}  # Gérer le cas où la moyenne est `None`


def histogramme_films_par_annee():
    pipeline = [{"$match": {"year": {"$exists": True}}},
                {"$group": {"_id": "$year", "count": {"$sum": 1}}}]
    data = list(films.aggregate(pipeline))
    df = pd.DataFrame(data)
    df = df.dropna()
    
    plt.figure(figsize=(12, 6))
    plt.bar(df["_id"].astype(int), df["count"])
    plt.xlabel("Année")
    plt.ylabel("Nombre de films")
    plt.title("Nombre de films par année")
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close()
    buf.seek(0)
    return buf

def genres_disponibles():
    try:
        # 1. Récupère toutes les entrées distinctes de genre
        genres_distincts = films.distinct("genre")
        
        # 2. Sépare et nettoie les genres
        genres_uniques = set()
        for genre_str in genres_distincts:
            if genre_str:  # Ignore les valeurs vides
                for genre in genre_str.split(','):
                    genre_nettoye = genre.strip()
                    if genre_nettoye:
                        genres_uniques.add(genre_nettoye)
        
        # 3. Retourne les genres triés
        return {"Genres uniques": sorted(genres_uniques)}
    
    except Exception as e:
        return {"Erreur": f"Erreur lors de la récupération des genres: {str(e)}"}
    
# requête6  
def film_plus_gros_revenu():
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
        
        # Gestion du cas où le revenu est None ou non numérique
        try:
            revenu_num = float(revenu) if revenu is not None else None
        except (ValueError, TypeError):
            revenu_num = None
        
        return {
            "Titre": film["Titre"],
            "Année": film["Année"],
            "Revenu": revenu_num,
            #"Revenu formaté": f"{revenu_num:,.2f} M$" if revenu_num is not None else "Non disponible"
        }
        
    except Exception as e:
        return {"Erreur": f"Erreur MongoDB: {str(e)}"}
    

# Requête 7
def realisateurs_plus_de_5_films():
    """
    Trouve les réalisateurs ayant réalisé plus de 5 films
    Retourne une liste de dictionnaires avec le nom du réalisateur et le nombre de films
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

# Requête 8
def genre_plus_rentable():
    """
    Trouve le genre de film le plus rentable en moyenne
    Retourne un dictionnaire avec le genre et le revenu moyen
    """
    pipeline = [
        {"$unwind": "$Genre"},  # Dédouble les films avec plusieurs genres
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
    """Récupère les revenus moyens pour tous les genres (pour la visualisation)"""
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


# Requête 9
def top_films_par_decennie():
    """
    Trouve les 3 meilleurs films par décennie depuis 1990.
    Classement : 
    1. Films "G" > "unrated"
    2. En cas d'égalité, plus de Votes d'abord.
    """
    pipeline = [
        # Filtrer depuis 1990 et les films avec rating/Votes valides
        {
            "$match": {
                "year": {"$gte": 1990, "$exists": True},
                "rating": {"$in": ["G", "unrated"]},
                "Votes": {"$exists": True, "$ne": None}
            }
        },
        # Calculer la décennie (ex: 2016 → "2010-2019")
        {
            "$addFields": {
                "Decennie": {
                    "$concat": [
                        {"$toString": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}},
                        "-",
                        {"$toString": {"$add": [{"$subtract": ["$year", {"$mod": ["$year", 10]}]}, 9]}}
                    ]
                },
                # Priorité : "G"=1, "unrated"=2 (pour tri ascendant)
                "RatingPriority": {
                    "$cond": [{"$eq": ["$rating", "G"]}, 1, 2]
                }
            }
        },
        # Trier par : Décennie → Rating → Votes (décroissant)
        {
            "$sort": {
                "Decennie": 1,
                "RatingPriority": 1,
                "Votes": -1
            }
        },
        #Garder les 3 premiers films par décennie
        {
            "$group": {
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
            }
        },
        {
            "$project": {
                "Decennie": "$_id",
                "Top 3": {"$slice": ["$films", 3]},
                "_id": 0
            }
        }
    ]
    
    return list(films.aggregate(pipeline))

# Requête 10
def film_plus_long_par_genre():
    """
    Trouve le film le plus long (Runtime) pour chaque genre.
    - Un film multi-genres est évalué dans chaque genre.
    - En cas d'égalité, le film avec le plus de votes est choisi.
    """
    pipeline = [
        #Filtre les films avec Runtime et genre valides
        {
            "$match": {
                "Runtime (Minutes)": {"$exists": True, "$ne": None},
                "genre": {"$exists": True, "$ne": None}
            }
        },
        #Sépare les genres (ex: "Crime,Horror" → ["Crime", "Horror"])
        {
            "$addFields": {
                "Genres": {
                    "$split": ["$genre", ","]
                }
            }
        },
        #Dédouble les films multi-genres (1 entrée par genre)
        {
            "$unwind": "$Genres"
        },
        #Nettoye les espaces autour des genres (ex: " Horror " → "Horror")
        {
            "$addFields": {
                "Genres": {"$trim": {"input": "$Genres"}}
            }
        },
        #Trie par genre → Runtime décroissant → Votes décroissant
        {
            "$sort": {
                "Genres": 1,
                "Runtime (Minutes)": -1,
                "Votes": -1
            }
        },
        #Garde le film le plus long par genre
        {
            "$group": {
                "_id": "$Genres",
                "Titre": {"$first": "$title"},
                "Runtime": {"$first": "$Runtime (Minutes)"},
                "Année": {"$first": "$year"},
                "Réalisateur": {"$first": "$Director"},
                "Votes": {"$first": "$Votes"}
            }
        },
        #Formate le résultat
        {
            "$project": {
                "_id": 0,
                "Genre": "$_id",
                "Titre": 1,
                "Durée (min)": "$Runtime",
                "Année": 1,
                "Réalisateur": 1,
                "Votes": 1
            }
        },
        #Trie par ordre alphabétique des genres
        {
            "$sort": {"Genre": 1}
        }
    ]
    
    return list(films.aggregate(pipeline))


# Requête 11
def creer_vue_films_premium(db):
    """
    Crée une vue MongoDB des films premium
    Args:
        db: Objet de base de données MongoDB
    """
    view_name = "films_premium"
    pipeline = [
        {
            "$match": {
                "Metascore": {"$gt": 80, "$exists": True},
                "Revenue (Millions)": {"$gt": 50, "$exists": True}
            }
        },
        {
            "$project": {
                "_id": 1,
                "title": 1,
                "year": 1,
                "Metascore": 1,
                "Revenue (Millions)": 1,
                "Director": 1,
                "rating": 1
            }
        }
    ]
    
    if view_name not in db.list_collection_names():
        db.command({
            "create": view_name,
            "viewOn": "films", 
            "pipeline": pipeline
        })
        return True
    return False


# Requête 12
def correlation_duree_revenu():
    """
    Calcule la corrélation entre durée et revenu.
    Gère les cas où Revenue (Millions) est :
    - Un nombre (ex: 341.26)
    - Une chaîne avec virgule (ex: "341,26")
    - Une chaîne vide ("")
    """
    pipeline = [
        {
            "$match": {
                "Runtime (Minutes)": {"$exists": True, "$ne": None, "$type": "number"},
                "Revenue (Millions)": {"$exists": True, "$ne": None, "$ne": ""}
            }
        },
        {
            "$addFields": {
                "RevenueClean": {
                    "$cond": [
                        {"$eq": [{"$type": "$Revenue (Millions)"}, "string"]},
                        {"$toDouble": {
                            "$replaceOne": {
                                "input": "$Revenue (Millions)",
                                "find": ",",
                                "replacement": "."
                            }
                        }},
                        "$Revenue (Millions)"
                    ]
                }
            }
        },
        {
            "$match": {
                "RevenueClean": {"$type": "number"}
            }
        },
        {
            "$project": {
                "Titre": "$title",
                "Runtime": "$Runtime (Minutes)",
                "Revenue": "$RevenueClean"
            }
        }
    ]

    # Récupération des données
    data = list(films.aggregate(pipeline))
    df = pd.DataFrame(data)

    if df.empty:
        return {"error": "Aucune donnée valide après nettoyage."}

    # Calcul de la corrélation
    corr = df["Runtime"].corr(df["Revenue"])

    # Visualisation
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
    
    
# Requête 13
def duree_moyenne_par_decennie():
    """
    Calcule la durée moyenne des films par décennie.
    Retourne un dictionnaire {décennie: durée_moyenne}.
    """
    pipeline = [
        {
            "$match": {
                "year": {"$gte": 1990, "$exists": True},
                "Runtime (Minutes)": {"$exists": True, "$ne": None, "$type": "number"}
            }
        },
        {
            "$addFields": {
                "decennie": {
                    "$concat": [
                        {"$toString": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}},
                        "-",
                        {"$toString": {"$add": [{"$subtract": ["$year", {"$mod": ["$year", 10]}]}, 9]}}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$decennie",
                "duree_moyenne": {"$avg": "$Runtime (Minutes)"},
                "count": {"$sum": 1}  # Nombre de films par décennie
            }
        },
        {
            "$project": {
                "decennie": "$_id",
                "duree_moyenne": {"$round": ["$duree_moyenne", 1]},  # Arrondi à 1 décimale
                "count": 1,
                "_id": 0
            }
        },
        {
            "$sort": {"decennie": 1}
        }
    ]
    
    results = list(films.aggregate(pipeline))
    return {res["decennie"]: res["duree_moyenne"] for res in results}



############# exportation des données vers neo4j
def exporter_donnees_pour_neo4j():
    """Exporte les données nécessaires pour Neo4j"""
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