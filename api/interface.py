import streamlit as st
import requests

# URLs de l'API FastAPI
MOVIE_API_URL = "http://localhost:8000/ingest/movie"
REVIEW_API_URL = "http://localhost:8000/ingest/review"

st.title("📽️ Ingestion de Données - Films & Critiques")

# Sélection du type de données à ingérer
option = st.radio("Que voulez-vous ajouter ?", ("Film", "Critique"))

# 🟢 FORMULAIRE POUR LES FILMS
if option == "Film":
    st.subheader("🎬 Ajouter un Film")

    name = st.text_input("Nom du film")
    year = st.text_input("Année")
    movie_rated = st.text_input("Classification (ex: PG-13)")
    run_length = st.text_input("Durée (ex: 148 min)")
    genres = st.text_input("Genres (ex: Action, Sci-Fi)")
    release_date = st.text_input("Date de sortie (ex: 2010-07-16)")
    rating = st.text_input("Note (ex: 8.8)")
    num_raters = st.text_input("Nombre de votants")
    num_reviews = st.text_input("Nombre de critiques")
    review_url = st.text_input("URL IMDB")

    if st.button("📤 Envoyer le film"):
        if not name or not year or not rating:
            st.error("❌ Veuillez remplir au moins le Nom, l'Année et la Note.")
        else:
            data = {
                "name": name,
                "year": year,
                "movie_rated": movie_rated,
                "run_length": run_length,
                "genres": genres,
                "release_date": release_date,
                "rating": rating,
                "num_raters": num_raters,
                "num_reviews": num_reviews,
                "review_url": review_url
            }
            response = requests.post(MOVIE_API_URL, json=data)

            if response.status_code == 200:
                st.success(f"✅ Film ajouté avec succès !\n📂 Clé S3: {response.json()['key']}")
            else:
                st.error(f"❌ Erreur : {response.text}")

# 🔵 FORMULAIRE POUR LES CRITIQUES
elif option == "Critique":
    st.subheader("📝 Ajouter une Critique")

    username = st.text_input("Nom d'utilisateur")
    rating = st.text_input("Note")
    helpful = st.text_input("Nombre d'utilisateurs ayant trouvé utile")
    total = st.text_input("Nombre total d'avis")
    date = st.text_input("Date de la critique (ex: 2025-03-09)")
    title = st.text_input("Titre de la critique")
    review = st.text_area("Contenu de la critique")
    movie_title = st.text_input("Titre du film")

    if st.button("📤 Envoyer la critique"):
        if not username or not rating or not title or not movie_title:
            st.error("❌ Veuillez remplir au moins le Nom d'utilisateur, la Note et le Titre.")
        else:
            data = {
                "username": username,
                "rating": rating,
                "helpful": helpful,
                "total": total,
                "date": date,
                "title": title,
                "review": review,
                "movie_title": movie_title
            }
            response = requests.post(REVIEW_API_URL, json=data)

            if response.status_code == 200:
                st.success(f"✅ Critique ajoutée avec succès !\n📂 Clé S3: {response.json()['key']}")
            else:
                st.error(f"❌ Erreur : {response.text}")
