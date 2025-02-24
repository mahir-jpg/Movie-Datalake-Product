import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# -- PARAMÈTRES MYSQL (Staging) --
MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "staging_db"
MYSQL_TABLE = "movies"

# -- PARAMÈTRES ELASTICSEARCH (Curated) --
ES_HOST = "http://elasticsearch:9200"
ES_INDEX_NAME = "my_curated_index"


def fetch_data_from_mysql():
    """
    Se connecte à la base MySQL (staging) et récupère toutes les lignes
    de la table spécifiée.
    """
    cnx = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = cnx.cursor(dictionary=True)

    query = f"SELECT * FROM {MYSQL_TABLE}"
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    return rows


def transform_data(data):
    """
    Ici, vous pouvez appliquer toutes vos transformations / nettoyages 
    sur les documents avant de les indexer dans Elasticsearch.
    """
    # EXEMPLE de transformation :
    # for doc in data:
    #     doc["combined_title"] = f"{doc['title']} ({doc['year']})"

    return data


def index_data_into_es(data):
    """
    Indexe (insère) un ensemble de documents (data) dans Elasticsearch via l'API bulk.
    """
    es = Elasticsearch(ES_HOST)

    # Crée l'index s'il n'existe pas déjà
    if not es.indices.exists(index=ES_INDEX_NAME):
        es.indices.create(index=ES_INDEX_NAME)

    # Préparation des actions pour le bulk
    actions = []
    for doc in data:
        actions.append({
            "_index": ES_INDEX_NAME,
            "_source": doc
        })

    # Ingestion en bulk
    success, _ = bulk(es, actions)
    print(f"Indexed {success} documents in index '{ES_INDEX_NAME}'.")


def main():
    # 1) Récupérer la donnée depuis MySQL (staging)
    data = fetch_data_from_mysql()

    # 2) Transformer les données si besoin (cette étape est optionnelle, mais c'est ici qu'on peut faire du "curated")
    data = transform_data(data)

    # 3) Indexer les données dans Elasticsearch (curated)
    index_data_into_es(data)


if __name__ == "__main__":
    main()
