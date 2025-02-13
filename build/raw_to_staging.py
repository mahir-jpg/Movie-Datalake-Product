import csv
import os
import re

import boto3
import mysql.connector

# Paramètres de connexion MySQL
#############################################
DB_HOST = "localhost"
DB_USER = "user"
DB_PASSWORD = "password"
DB_NAME = "staging_db"


# Paramètres pour LocalStack (S3)
#############################################
S3_ENDPOINT_URL = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "root"
AWS_SECRET_ACCESS_KEY = "root"
BUCKET_NAME = "raw"


# Dossiers S3
###############################################
MOVIES_PREFIX = "1_movies_per_genre/"
REVIEWS_PREFIX = "2_reviews_per_movie_raw/"


def get_db_connection():
    """Connexion MySQL"""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


def get_s3_client():
    """Connexion S3 (LocalStack)"""
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )



# Insertions MOVIES 
##################################################
def insert_movie(row, cursor):
    """
    Insère un film dans la table 'movies'
    Fusionne 'name' et 'year' en 'film_title'
    """
    name = row[0].strip()
    year = row[1].strip()

    if not name or not year:
        return  # Skip ligne invalide

    film_title = f"{name} {year}"  # Fusion name + year

    movie_rated = row[2].strip()
    run_length = row[3].strip()
    genres = row[4].strip()
    release_date = row[5].strip()
    rating = float(row[6]) if row[6] else None
    num_raters = int(row[7]) if row[7] else None
    num_reviews = int(row[8]) if row[8] else None
    review_url = row[9].strip() if row[9] else None

    sql = """
        INSERT INTO movies (
            film_title, movie_rated, run_length, genres, 
            release_date, rating, num_raters, num_reviews, review_url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE rating = VALUES(rating);
    """
    cursor.execute(sql, (
        film_title, movie_rated, run_length, genres,
        release_date, rating, num_raters, num_reviews, review_url
    ))


def process_movies_csv(s3_client, db_cursor, key):
    """
    Traite un fichier CSV dans '1_movies_per_genre/'
    """
    print(f"--- Traitement du CSV (movies): s3://{BUCKET_NAME}/{key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj['Body'].read().decode('utf-8').splitlines()

    reader = csv.reader(content)
    next(reader, None)  # Skip l'en-tête

    for row in reader:
        if len(row) < 10:
            continue
        insert_movie(row, db_cursor)



# Insertions REVIEWS (utilisation de Bulk Insert pour accelérer le traitement)
########################################################

def bulk_insert_reviews(review_rows, film_title, cursor):
    """
    Effectue un INSERT en bulk pour toutes les lignes de `review_rows`.
    Chaque élément de review_rows est un tuple : (username, rating, helpful, total, date, title, review).
    On construit une seule requête INSERT multi-values.
    """
    if not review_rows:
        return

    placeholders = []
    values = []
    for row in review_rows:
        placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s)")
        # row = (username, rating, helpful, total, date, title, review)
        # On ajoute film_title à la fin
        values.extend(row + (film_title,))

    sql = """
    INSERT INTO reviews (
        username, rating, helpful, total, date, title, review, film_title
    )
    VALUES 
    """ + ",".join(placeholders)

    cursor.execute(sql, tuple(values))


def process_reviews_csv(s3_client, db_conn, db_cursor, key):
    """
    Traite un fichier CSV dans '2_reviews_per_movie_raw/'
    On déduit 'film_title' du nom de fichier. 
    On fait de l'insertion en bulk par lot (ex: 1000 lignes).
    """
    import re

    CHUNK_SIZE = 5000
    batch = []

    file_name = os.path.basename(key)  # Ex: "The Prestige 2006.csv"
    film_title = re.sub(r"\.csv$", "", file_name, flags=re.IGNORECASE)

    print(f"--- Traitement du CSV (reviews): s3://{BUCKET_NAME}/{key}")
    print(f"      -> film_title = '{film_title}'")

    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj['Body'].read().decode('utf-8').splitlines()

    reader = csv.reader(content)
    next(reader, None)  # Skip l'en-tête (username,rating,helpful,total,date,title,review,...)

    for row in reader:
        if len(row) < 7:
            continue

        username = row[0].strip()

        # Gestion des colonnes Null
        rating_str = row[1].strip()
        if rating_str.lower() == "null" or rating_str == "":
            rating = None
        else:
            rating = float(rating_str)

        helpful = int(row[2]) if row[2] else None
        total = int(row[3]) if row[3] else None
        date = row[4].strip()
        title = row[5]
        review = row[6]

        # Ajouter la ligne à la batch
        batch.append((username, rating, helpful, total, date, title, review))

        # Si on atteint CHUNK_SIZE, on bulk insert
        if len(batch) >= CHUNK_SIZE:
            bulk_insert_reviews(batch, film_title, db_cursor)
            db_conn.commit()
            batch.clear()

    # Insérer les dernières lignes (moins de CHUNK_SIZE)
    if batch:
        bulk_insert_reviews(batch, film_title, db_cursor)
        db_conn.commit()
        batch.clear()

def list_all_objects(s3_client, bucket, prefix):
    """
    Retourne la liste complète (tous les objets) pour un Bucket/Prefix, en gérant la pagination s3 (list_objects_v2 est limitée a 1000 éléments)
    """
    all_objects = []
    continuation_token = None

    while True:
        if continuation_token:
            resp = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            resp = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )

        if 'Contents' in resp:
            all_objects.extend(resp['Contents'])

        if resp.get('IsTruncated'):
            continuation_token = resp.get('NextContinuationToken')
        else:
            break

    return all_objects


# MAIN
#########################################################
def main():
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    s3_client = get_s3_client()

    # 1) Charger les films (insert row-by-row, moins volumineux)
    resp_movies = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=MOVIES_PREFIX)
    if 'Contents' in resp_movies:
        for item in resp_movies['Contents']:
            key = item['Key']
            if key.endswith(".csv"):
                process_movies_csv(s3_client, cursor, key)
                db_conn.commit()

    # 2) Charger les reviews (bulk insert par batch de 1000)
    resp_reviews = list_all_objects(s3_client, bucket=BUCKET_NAME, prefix=REVIEWS_PREFIX)
    for item in resp_reviews:
        key = item['Key']
        if key.endswith(".csv"):
            process_reviews_csv(s3_client, db_conn, cursor, key)
                
    print("Noice")
    cursor.close()
    db_conn.close()


if __name__ == "__main__":
    main()
