import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import time

# -- PARAMÈTRES MYSQL (Staging) --
MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "staging_db"

# -- PARAMÈTRES ELASTICSEARCH (Curated) --
ES_HOST = "http://elasticsearch:9200"
REVIEWS_INDEX = "reviews_index_curated"

def fetch_data_from_mysql_gen(table_name, chunk_size=10000):
    """
    Générateur qui se connecte à MySQL et récupère les lignes de la table
    par chunks (pagination).
    """
    cnx = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = cnx.cursor(dictionary=True)
    offset = 0
    while True:
        query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
        cursor.execute(query)
        chunk = cursor.fetchall()
        if not chunk:
            break
        yield chunk
        offset += chunk_size
    cursor.close()
    cnx.close()

def transform_reviews_for_curated(data):
    """
    Transformation pour les critiques, incluant uniquement les films avec un timestamp.
    Suppression du timestamp du titre dans les critiques.
    """
    for doc in data:
        if "film_title" in doc:
            # Enlever la partie après le premier underscore
            clean_title = doc["film_title"].split('_')[0]  # On garde juste la partie avant le premier '_'
            doc["clean_film_title"] = clean_title
            doc["review_length"] = len(doc["review"]) if "review" in doc else 0
        else:
            doc["clean_film_title"] = ""  # Si pas de film_title, mettre une chaîne vide.
    return data

def index_data_into_es_streaming(es, data, index_name, batch_size=2000):
    """
    Indexe les documents dans Elasticsearch via streaming_bulk en traitant par batches.
    Affiche un retour de progression pour chaque batch.
    """
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    
    total_indexed = 0
    actions = []
    
    for doc in data:
        if "clean_film_title" in doc:
            doc_id = f"{doc['username']}_{doc['clean_film_title']}"  # Utilisation de clean_film_title pour l'ID
            actions.append({
                "_op_type": "index",
                "_index": index_name,
                "_id": doc_id,
                "_source": doc
            })
        else:
            continue  # Ignorer les documents sans clean_film_title
        
    # Utilisation de streaming_bulk pour indexer en batch
    counter = 0
    for ok, info in streaming_bulk(es, actions, chunk_size=batch_size):
        counter += 1
        if ok:
            total_indexed += 1
        else:
            print(f"Erreur sur le document {counter}: {info}")
        # Affiche la progression pour chaque batch complet
        if counter % batch_size == 0:
            print(f"{counter} documents traités dans ce lot, total indexés: {total_indexed}.")
    
    return total_indexed

def process_table_streaming(table_name, transform_func, index_name, es, chunk_size=10000, batch_size=2000):
    """
    Traite une table MySQL en lisant par chunks, en appliquant une transformation
    et en indexant dans Elasticsearch.
    Affiche un retour de progression pour chaque chunk traité.
    """
    total_indexed = 0
    chunk_count = 0
    for chunk in fetch_data_from_mysql_gen(table_name, chunk_size):
        chunk_count += 1
        print(f"Traitement du chunk {chunk_count} avec {len(chunk)} enregistrements.")
        transformed_chunk = transform_func(chunk)
        indexed_this_chunk = index_data_into_es_streaming(es, transformed_chunk, index_name, batch_size)
        total_indexed += indexed_this_chunk
        print(f"[{table_name}] Chunk {chunk_count}: {len(chunk)} enregistrements traités, {indexed_this_chunk} documents indexés (Total indexés: {total_indexed}).")
    return total_indexed

def main():
    # Initialisation du client Elasticsearch avec des paramètres de timeout et retry
    es = Elasticsearch([ES_HOST], timeout=180, max_retries=5, retry_on_timeout=True)
    
    print("Traitement de la table 'reviews'...")
    reviews_indexed = process_table_streaming("reviews", transform_reviews_for_curated, REVIEWS_INDEX, es)
    
    print(f"Indexation terminée. Total critiques indexées: {reviews_indexed}.")

if __name__ == "__main__":
    main()
