import os
import pandas as pd
import boto3

# Configuration
def merge_and_upload(source_dirs, bucket, endpoint):
    """
    Fusionne les fichiers CSV des dossiers source et les upload vers S3.
    
    Args:
    - source_dirs (list): Liste des dossiers contenant les fichiers à fusionner.
    - bucket (str): Nom du bucket S3 de destination.
    - endpoint (str): URL de l'endpoint LocalStack.
    """
    s3_client = boto3.client("s3", endpoint_url=endpoint)

    # Initialisation des DataFrames pour les fichiers combinés
    movies_df = pd.DataFrame()
    reviews_df = pd.DataFrame()

    for source_dir in source_dirs:
        for file_name in os.listdir(source_dir):
            if file_name.endswith(".csv"):
                file_path = os.path.join(source_dir, file_name)
                
                # Lire le fichier CSV
                df = pd.read_csv(file_path)
                
                # Fusionner les fichiers dans la bonne catégorie
                if "movies" in source_dir.lower():
                    movies_df = pd.concat([movies_df, df], ignore_index=True)
                elif "reviews" in source_dir.lower():
                    # Extraire le titre du film à partir du nom du fichier
                    movie_title = os.path.splitext(file_name)[0]
                    df["movie_title"] = movie_title  # Ajouter une colonne avec le titre
                    reviews_df = pd.concat([reviews_df, df], ignore_index=True)
    
    # Exporter les fichiers fusionnés en CSV temporaires
    movies_csv = "all_movies.csv"
    reviews_csv = "all_reviews.csv"
    movies_df.to_csv(movies_csv, sep=';', index=False)
    reviews_df.to_csv(reviews_csv, sep=';', index=False)
    
    # Upload les fichiers fusionnés vers le bucket S3
    s3_client.upload_file(movies_csv, bucket, "all_movies.csv")
    print(f"Uploaded {movies_csv} to s3://{bucket}/all_movies.csv")
    
    s3_client.upload_file(reviews_csv, bucket, "all_reviews.csv")
    print(f"Uploaded {reviews_csv} to s3://{bucket}/all_reviews.csv")

    # Supprimer les fichiers temporaires
    os.remove(movies_csv)
    os.remove(reviews_csv)

if __name__ == "__main__":
    import argparse

    # Argument parser
    parser = argparse.ArgumentParser(description="Fusionner et uploader des CSV vers S3")
    parser.add_argument("--source_dirs", nargs="+", required=True, help="Dossiers contenant les fichiers à traiter")
    parser.add_argument("--bucket", required=True, help="Nom du bucket S3 de destination")
    parser.add_argument("--endpoint", required=True, help="Endpoint LocalStack")

    args = parser.parse_args()

    # Appeler la fonction principale
    merge_and_upload(args.source_dirs, args.bucket, args.endpoint)
