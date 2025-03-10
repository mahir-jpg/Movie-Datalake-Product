from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3
import csv
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

app = FastAPI()

# Configuration S3 pour LocalStack
S3_BUCKET = "raw"
S3_ENDPOINT = "http://localhost:4566"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

# Vérification et création du bucket si nécessaire
@app.on_event("startup")
def startup_event():
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        print("no bucket")

# Modèles Pydantic
class Movie(BaseModel):
    name: str
    year: str
    movie_rated: str
    run_length: str
    genres: str
    release_date: str
    rating: str
    num_raters: str
    num_reviews: str
    review_url: str

class Review(BaseModel):
    username: str
    rating: str
    helpful: str
    total: str
    date: str
    title: str
    review: str
    movie_title: str

def create_csv_content(header: list, row: list) -> str:
    """
    Génère un contenu CSV sous forme de string.
    """
    output = io.StringIO()
    writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(header)
    writer.writerow(row)
    return output.getvalue()

def upload_to_s3(file_key: str, csv_content: str):
    """
    Fonction pour uploader un fichier sur S3 en parallèle.
    """
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=csv_content.encode("utf-8"))
        return {"message": "Succès", "key": file_key}
    except Exception as e:
        return {"error": str(e)}

executor = ThreadPoolExecutor(max_workers=4)  # Parallélisation avec 4 threads

@app.post("/ingest_fast/movie")
def ingest_fast_movie(movie: Movie):
    """
    Endpoint optimisé pour ingérer les données d'un film.
    """
    try:
        movie_data = movie.dict()
        header = ["name", "year", "movie_rated", "run_length", "genres", "release_date", "rating", "num_raters", "num_reviews", "review_url"]
        row = [movie_data[col] for col in header]
        csv_content = create_csv_content(header, row)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_key = f"1_movies_per_genre/{movie_data['name']} ({movie_data['year']})_{timestamp}.csv"

        # Lancement en parallèle
        future = executor.submit(upload_to_s3, file_key, csv_content)
        return future.result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest_fast/review")
def ingest_fast_review(review: Review):
    """
    Endpoint optimisé pour ingérer les critiques.
    """
    try:
        review_data = review.dict()
        header = ["username", "rating", "helpful", "total", "date", "title", "review", "movie_title"]
        row = [review_data[col] for col in header]
        csv_content = create_csv_content(header, row)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_key = f"2_reviews_per_movie_raw/{review_data['movie_title']}_{timestamp}.csv"

        # Lancement en parallèle
        future = executor.submit(upload_to_s3, file_key, csv_content)
        return future.result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
