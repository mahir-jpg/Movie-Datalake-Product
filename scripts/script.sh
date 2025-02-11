#!/bin/bash

# Vérification de Docker
if ! command -v docker &> /dev/null; then
    echo "Docker n'est pas installé."
fi

# Vérification d'AWS CLI
if ! command -v aws &> /dev/null; then
    echo "AWS CLI n'est pas installé."
fi

# Configuration d'AWS CLI pour LocalStack
echo "Configuration d'AWS CLI pour LocalStack..."
aws configure set aws_access_key_id root
aws configure set aws_secret_access_key root
aws configure set default.region us-east-1


# Création des buckets S3 (raw, staging, curated)
echo "Création du bucket S3: raw..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://raw

echo "Bucket créé avec succès !"

