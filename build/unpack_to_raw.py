import os
import shutil
import argparse
from pathlib import Path

import boto3
from huggingface_hub import snapshot_download

# Repo Huggingface contenant les données
REPO_ID = "MansaT/Movie-Dataset"

def upload_directory_to_s3(local_dir: str, bucket: str, s3_client):
    """
    Parcourt un dossier local et upload tous les fichiers qu'il contient (récursivement)
    vers un bucket S3 sous un préfixe donné.
    """
    local_dir_path = Path(local_dir)
    for file_path in local_dir_path.rglob('*'):
        if file_path.is_file():
            # Calcul du chemin relatif pour respecter l'arborescence
            relative_path = file_path.relative_to(local_dir_path)
            s3_key = f"{relative_path}".replace('\\', '/')
            
            s3_client.upload_file(str(file_path), bucket, s3_key)
            print(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")


def download_and_upload_raw(bucket: str, endpoint: str, only_csv: bool = True):
    """
    Télécharge les données brutes depuis Hugging Face et les uploade telles quelles
    dans un bucket S3, sans aucune transformation.

    Args:
        bucket (str): Nom du bucket S3 où stocker les données brutes.
        endpoint (str): URL de l'endpoint (ex: LocalStack).
        only_csv (bool): Si True, ne télécharge que les fichiers CSV.
    """
    s3_client = boto3.client("s3", endpoint_url=endpoint)

    # Répertoire local temporaire (cache)
    local_cache_dir = "./hf_cache"

    # Option pour ne traiter que les csv
    allow_patterns = ["*.csv"] if only_csv else None

    # 1) Télécharge le repo Hugging Face dans un dossier local.
    #    only_csv=True, on ne prend que les fichiers CSV.
    local_repo_dir = snapshot_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        local_dir=local_cache_dir,
        allow_patterns=allow_patterns
    )

    # 2) Upload de tout le contenu téléchargé vers S3
    upload_directory_to_s3(local_repo_dir, bucket, s3_client=s3_client)

    # 3) Nettoyage du répertoire local
    shutil.rmtree(local_repo_dir, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Télécharger depuis Hugging Face et uploader tel quel dans S3.")
    parser.add_argument("--bucket", required=True, help="Nom du bucket S3 de destination")
    parser.add_argument("--endpoint", required=True, help="Endpoint LocalStack (ou autre)")

    # Option pour ne pas restreindre aux CSV uniquement
    parser.add_argument("--all_files", action="store_true",
                        help="Si spécifié, télécharge tous les fichiers du repo (pas seulement les CSV).")

    args = parser.parse_args()

    # only_csv = False si l'utilisateur a mis --all_files
    download_and_upload_raw(args.bucket, args.endpoint, only_csv=not args.all_files)
