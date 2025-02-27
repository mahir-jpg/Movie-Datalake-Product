from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3
import csv
import io
from uuid import uuid4

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

# À l'initialisation, on vérifie si le bucket existe sinon on le crée.
@app.on_event("startup")
def startup_event():
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception as e:
        print("no bucket")

# Modèle pour les données de films (movies per genre)
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

# Modèle pour les données de critiques (reviews per movie)
class Review(BaseModel):
    username: str
    rating: str
    helpful: str
    total: str
    date: str
    title: str
    review: str

def create_csv_content(header: list, row: list) -> str:
    """
    Crée un contenu CSV à partir d'une liste d'en-têtes et d'une liste de valeurs.
    """
    output = io.StringIO()
    writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(header)
    writer.writerow(row)
    return output.getvalue()

@app.post("/ingest/movie")
def ingest_movie(movie: Movie):
    """
    Endpoint pour ingérer les données d'un film dans le dossier "1_movies_per_genre_raw".
    """
    try:
        movie_data = movie.dict()
        # Définition des en-têtes CSV attendues
        header = ["name", "year", "movie_rated", "run_length", "genres", "release_date", "rating", "num_raters", "num_reviews", "review_url"]
        row = [movie_data[col] for col in header]
        csv_content = create_csv_content(header, row)
        file_key = f"1_movies_per_genre/{uuid4()}.csv"
        s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=csv_content.encode("utf-8"))
        return {"message": "Données du film ingérées avec succès", "key": file_key}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/review")
def ingest_review(review: Review):
    """
    Endpoint pour ingérer les données d'une critique dans le dossier "2_reviews_per_movie_raw".
    """
    try:
        review_data = review.dict()
        header = ["username", "rating", "helpful", "total", "date", "title", "review"]
        row = [review_data[col] for col in header]
        csv_content = create_csv_content(header, row)
        file_key = f"2_reviews_per_movie_raw/{uuid4()}.csv"
        s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=csv_content.encode("utf-8"))
        return {"message": "Données de la critique ingérées avec succès", "key": file_key}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 