import csv
import re
import os
import boto3
import mysql.connector
import time
import io

# Paramètres MySQL
DB_HOST = "mysql"
DB_USER = "user"
DB_PASSWORD = "password"
DB_NAME = "staging_db"

# Paramètres LocalStack S3
S3_ENDPOINT_URL = "http://localstack:4566"
AWS_ACCESS_KEY_ID = "root"
AWS_SECRET_ACCESS_KEY = "root"
BUCKET_NAME = "raw"

# Préfixes S3
MOVIES_PREFIX = "1_movies_per_genre/"
REVIEWS_PREFIX = "2_reviews_per_movie_raw/"

# Regex pour capturer les fichiers avec timestamp suffixe (_YYYYMMDD_HHMMSS.csv)
TIMESTAMP_PATTERN = re.compile(r".+_\d{8}_\d{6}\.csv$")

# Taille des batchs
MOVIE_BATCH_SIZE = 100
REVIEW_BATCH_SIZE = 5000


def get_db_connection():
    """Connexion à MySQL"""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


def get_s3_client():
    """Connexion S3 via LocalStack"""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )


# ----- INSERTION DES FILMS -----

def insert_movies(movie_rows, cursor):
    """Insère plusieurs films en une seule requête avec executemany()"""
    sql = """
        INSERT INTO movies (
            film_title, movie_rated, run_length, genres, 
            release_date, rating, num_raters, num_reviews, review_url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE rating = VALUES(rating);
    """
    cursor.executemany(sql, movie_rows)


def process_movies_csv(s3_client, db_conn, cursor, key):
    """Traite un fichier CSV de films"""
    print(f"--- Traitement du CSV (movies): s3://{BUCKET_NAME}/{key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    content = io.StringIO(obj["Body"].read().decode("utf-8"))

    reader = csv.reader(content)
    next(reader, None)  # Skip l'en-tête

    batch = []
    for row in reader:
        if len(row) < 10:
            continue
        
        name = row[0].strip()
        year = row[1].strip()
        if not name or not year:
            continue  # Skip ligne invalide

        film_title = f"{name} {year}"
        movie_rated = row[2].strip()
        run_length = row[3].strip()
        genres = row[4].strip()
        release_date = row[5].strip()
        rating = float(row[6]) if row[6] else None
        num_raters = int(row[7]) if row[7] else None
        num_reviews = int(row[8]) if row[8] else None
        review_url = row[9].strip() if row[9] else None

        batch.append((
            film_title, movie_rated, run_length, genres,
            release_date, rating, num_raters, num_reviews, review_url
        ))

        if len(batch) >= MOVIE_BATCH_SIZE:
            insert_movies(batch, cursor)
            db_conn.commit()
            batch.clear()

    if batch:
        insert_movies(batch, cursor)
        db_conn.commit()


# ----- INSERTION DES REVIEWS -----

def bulk_insert_reviews(review_rows, cursor):
    """Effectue un INSERT en bulk pour toutes les reviews avec executemany()"""
    if not review_rows:
        return

    sql = """
        INSERT IGNORE INTO reviews (
            username, rating, helpful, total, date, title, review, film_title
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.executemany(sql, review_rows)


def process_reviews_csv(s3_client, db_conn, cursor, key):
    """Traite un fichier CSV de reviews"""
    file_name = os.path.basename(key)
    film_title = re.sub(r"\.csv$", "", file_name, flags=re.IGNORECASE)

    print(f"--- Traitement du CSV (reviews): s3://{BUCKET_NAME}/{key}")
    print(f"      -> film_title = '{film_title}'")

    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    content = io.StringIO(obj["Body"].read().decode("utf-8"))

    reader = csv.reader(content)
    next(reader, None)  # Skip l'en-tête

    batch = []
    for row in reader:
        if len(row) < 7:
            continue

        username = row[0].strip()
        rating = float(row[1]) if row[1] and row[1].lower() != "null" else None
        helpful = int(row[2]) if row[2] else None
        total = int(row[3]) if row[3] else None
        date = row[4].strip()
        title = row[5].strip()
        review = row[6].strip()

        batch.append((username, rating, helpful, total, date, title, review, film_title))

        if len(batch) >= REVIEW_BATCH_SIZE:
            bulk_insert_reviews(batch, cursor)
            db_conn.commit()
            batch.clear()

    if batch:
        bulk_insert_reviews(batch, cursor)
        db_conn.commit()


def list_all_objects(s3_client, bucket, prefix):
    """Récupère tous les objets S3 avec gestion de la pagination"""
    all_objects = []
    continuation_token = None

    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**params)

        if 'Contents' in resp:
            all_objects.extend(resp['Contents'])

        if resp.get('IsTruncated'):
            continuation_token = resp.get('NextContinuationToken')
        else:
            break

    return all_objects


# ----- MAIN -----

def main():
    """Pipeline d'ingestion des films et des reviews"""
    db_conn = get_db_connection()
    start_time = time.time()
    cursor = db_conn.cursor()
    s3_client = get_s3_client()

    # 1) Charger les FILMS (batch insert)
    resp_movies = list_all_objects(s3_client, BUCKET_NAME, MOVIES_PREFIX)
    for item in resp_movies:
        key = item["Key"]
        if TIMESTAMP_PATTERN.match(key):
            process_movies_csv(s3_client, db_conn, cursor, key)

    # 2) Charger les REVIEWS (batch insert)
    resp_reviews = list_all_objects(s3_client, BUCKET_NAME, REVIEWS_PREFIX)
    for item in resp_reviews:
        key = item["Key"]
        if TIMESTAMP_PATTERN.match(key):
            process_reviews_csv(s3_client, db_conn, cursor, key)

    print("\n🏁 Ingestion terminée en {:.2f} sec".format(time.time() - start_time))
    cursor.close()
    db_conn.close()


if __name__ == "__main__":
    main()
