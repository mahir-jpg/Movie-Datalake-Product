CREATE DATABASE IF NOT EXISTS staging_db;
USE staging_db;

-- Table pour les films (modifiée)
CREATE TABLE IF NOT EXISTS movies (
    film_title VARCHAR(255) PRIMARY KEY,  -- Clé primaire unique pour chaque film
    movie_rated VARCHAR(10),
    run_length VARCHAR(50),
    genres TEXT,
    release_date VARCHAR(255),
    rating DECIMAL(3,1),
    num_raters INT,
    num_reviews INT,
    review_url VARCHAR(500)
);

-- Table pour les reviews (modifiée)
CREATE TABLE IF NOT EXISTS reviews (
    username VARCHAR(255),
    film_title VARCHAR(255),
    rating DECIMAL(3,1),
    helpful INT,
    total INT,
    date VARCHAR(50),
    title TEXT,
    review TEXT,
    PRIMARY KEY (username, film_title)
);

