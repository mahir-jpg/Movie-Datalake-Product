import requests
import time
import random

# URLs des endpoints
MOVIE_URL = "http://localhost:8000/ingest_fast/movie"  
REVIEW_URL = "http://localhost:8000/ingest_fast/review"

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

# Exemple de données pour une review
review_template = {
    "username": "user123",
    "rating": "8",
    "helpful": "15",
    "total": "20",
    "date": "2025-03-10",
    "title": "Amazing Movie!",
    "review": "This movie was absolutely fantastic, highly recommended!",
    "movie_title": "Test Movie"
}

# Nombre de requêtes
NUM_MOVIES = 500
REVIEWS_PER_MOVIE = 4
NUM_REVIEWS = NUM_MOVIES * REVIEWS_PER_MOVIE  # 2000 reviews

# Stocker les temps de réponse
movie_response_times = []
review_response_times = []

# Mesurer le temps total
start_time = time.time()

# Envoyer les films
for i in range(NUM_MOVIES):
    movie_data = movie_data_template.copy()
    movie_data["name"] = f"Test Movie {i+1}"  # Modifier le nom pour chaque film

    req_start = time.time()
    response = requests.post(MOVIE_URL, json=movie_data)
    req_end = time.time()

    movie_response_times.append(req_end - req_start)

    print(f"[FILM] Req {i+1}: Status {response.status_code}, Temps {movie_response_times[-1]:.3f} sec")

# Envoyer les reviews
review_id = 1
for i in range(NUM_MOVIES):
    movie_title = f"Test Movie {i+1}"

    for j in range(REVIEWS_PER_MOVIE):
        review_data = review_template.copy()
        review_data["username"] = f"user{review_id}"
        review_data["rating"] = str(random.randint(1, 10))
        review_data["helpful"] = str(random.randint(0, 100))
        review_data["total"] = str(random.randint(1, 200))
        review_data["date"] = f"2025-03-{random.randint(1, 31)}"
        review_data["title"] = f"Review {review_id} for {movie_title}"
        review_data["review"] = f"This is a generated review number {review_id} for {movie_title}."
        review_data["movie_title"] = movie_title

        req_start = time.time()
        response = requests.post(REVIEW_URL, json=review_data)
        req_end = time.time()

        review_response_times.append(req_end - req_start)
        review_id += 1

        print(f"[REVIEW] Req {review_id}: Status {response.status_code}, Temps {review_response_times[-1]:.3f} sec")

end_time = time.time()

# Analyse des résultats
print("\n=== Résumé des performances ===")
print(f"- Temps total : {end_time - start_time:.2f} sec")
print(f"- Temps moyen par film : {sum(movie_response_times) / NUM_MOVIES:.3f} sec")
print(f"- Temps moyen par review : {sum(review_response_times) / NUM_REVIEWS:.3f} sec")
