import requests
import time

# URL de l'endpoint
URL = "http://localhost:8000/ingest/movie"  # Change si nécessaire

# Exemple de données pour un film
movie_data_template = {
    "name": "Test Movie",
    "year": "2025",
    "movie_rated": "PG-13",
    "run_length": "120 min",
    "genres": "Action, Sci-Fi",
    "release_date": "2025-06-20",
    "rating": "8.5",
    "num_raters": "100000",
    "num_reviews": "5000",
    "review_url": "http://example.com"
}

# Nombre de requêtes à envoyer
NUM_REQUESTS = 500

# Liste pour stocker les temps de réponse
response_times = []

# Mesurer le temps total
start_time = time.time()

for i in range(NUM_REQUESTS):
    movie_data = movie_data_template.copy()
    movie_data["name"] = f"Test Movie {i+1}"  # Modifier le nom pour chaque requête

    req_start = time.time()
    response = requests.post(URL, json=movie_data)
    req_end = time.time()

    response_times.append(req_end - req_start)
    
    print(f"Req {i+1}: Status {response.status_code}, Temps {response_times[-1]:.3f} sec")

end_time = time.time()

# Analyse des résultats
print("\n=== Résumé des performances ===")
print(f"- Temps total : {end_time - start_time:.2f} sec")
print(f"- Temps moyen par requête : {sum(response_times) / NUM_REQUESTS:.3f} sec")
