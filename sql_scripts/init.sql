CREATE DATABASE IF NOT EXISTS staging_db;
USE staging_db;

-- Table pour les films (modifiée)
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    film_title VARCHAR(255) NOT NULL,  -- Fusion name + year (ex: "Inception 2010")
    movie_rated VARCHAR(10),
    run_length VARCHAR(50),
    genres TEXT,
    release_date VARCHAR(255),
    rating DECIMAL(3,1),
    num_raters INT,
    num_reviews INT,
    review_url VARCHAR(500),
    UNIQUE KEY unique_movie (film_title) -- Empêche les doublons
);

-- Table pour les reviews (inchangée)
CREATE TABLE IF NOT EXISTS reviews (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255),
    rating DECIMAL(3,1),
    helpful INT,
    total INT,
    date VARCHAR(50),
    title TEXT,
    review TEXT,
    film_title VARCHAR(255) 
);
