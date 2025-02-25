import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
import time

# -- PARAMÈTRES MYSQL (Staging) --
MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "staging_db"

# -- PARAMÈTRES ELASTICSEARCH (Gold) --
ES_HOST = "http://elasticsearch:9200"
MOVIES_INDEX = "movies_index"
REVIEWS_INDEX = "reviews_index"

def fetch_data_from_mysql_gen(table_name, chunk_size=1000):
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

def transform_movies(data):
    """
    Transforme les données des films, par exemple en créant un champ 'combined_title'.
    """
    for doc in data:
        if "title" in doc and "year" in doc:
            doc["combined_title"] = f"{doc['title']} ({doc['year']})"
        else:
            doc["combined_title"] = doc.get("title", "")
    return data

def transform_reviews(data):
    """
    Transforme les données des critiques, par exemple en calculant la longueur du texte.
    """
    for doc in data:
        doc["review_length"] = len(doc["review_text"]) if "review_text" in doc else 0
    return data

def index_data_into_es(es, data, index_name, batch_size=200):
    """
    Indexe les documents dans Elasticsearch via l'API bulk en traitant par batches.
    En cas d'erreur (ex: BulkIndexError), réessaie jusqu'à 3 fois avec un délai de 5 secondes.
    Si après 3 tentatives, le batch pose toujours problème, il affiche les erreurs et passe au batch suivant.
    """
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    total_indexed = 0
    actions = [{"_index": index_name, "_source": doc} for doc in data]
    for i in range(0, len(actions), batch_size):
        batch = actions[i : i + batch_size]
        attempts = 0
        success_in_batch = 0
        while attempts < 3:
            try:
                success, _ = bulk(es, batch)
                success_in_batch = success
                total_indexed += success
                break  # Batch traité avec succès, passer au suivant
            except BulkIndexError as bie:
                attempts += 1
                print(f"Batch {i//batch_size+1}: Tentative {attempts} échouée avec BulkIndexError: {bie}. Réessai dans 5 secondes...")
                time.sleep(5)
                if attempts == 3:
                    print(f"Batch {i//batch_size+1}: Échec définitif. Documents non indexés:")
                    for error in bie.errors:
                        print(error)
                    # On passe au batch suivant
                    break
            except Exception as e:
                attempts += 1
                print(f"Batch {i//batch_size+1}: Tentative {attempts} échouée avec l'erreur: {e}. Réessai dans 5 secondes...")
                time.sleep(5)
                if attempts == 3:
                    print(f"Batch {i//batch_size+1}: Échec définitif, passage au batch suivant.")
                    break
        print(f"Batch {i//batch_size+1} terminé: {success_in_batch} documents indexés dans ce batch.")
    return total_indexed

def process_table(table_name, transform_func, index_name, es, chunk_size=1000, batch_size=200):
    """
    Traite une table MySQL en lisant par chunks, en appliquant une transformation
    et en indexant dans Elasticsearch.
    Affiche un retour de progression pour chaque chunk traité.
    """
    total_indexed = 0
    chunk_count = 0
    for chunk in fetch_data_from_mysql_gen(table_name, chunk_size):
        chunk_count += 1
        transformed_chunk = transform_func(chunk)
        indexed_this_chunk = index_data_into_es(es, transformed_chunk, index_name, batch_size)
        total_indexed += indexed_this_chunk
        print(f"[{table_name}] Chunk {chunk_count}: Traitement de {len(chunk)} enregistrements, indexation de {indexed_this_chunk} documents (Total indexés: {total_indexed}).")
    return total_indexed

def main():
    # Initialisation du client Elasticsearch avec un timeout prolongé et des paramètres de retry
    es = Elasticsearch([ES_HOST], timeout=180, max_retries=5, retry_on_timeout=True)
    print("Traitement de la table 'movies'...")
    movies_indexed = process_table("movies", transform_movies, MOVIES_INDEX, es)
    print("Traitement de la table 'reviews'...")
    reviews_indexed = process_table("reviews", transform_reviews, REVIEWS_INDEX, es)
    print(f"Indexation terminée. Total films indexés: {movies_indexed}, total critiques indexées: {reviews_indexed}.")

if __name__ == "__main__":
    main()
