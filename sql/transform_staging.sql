BEGIN;

--A) Load new movies
INSERT INTO movies (movie_id, title, genres)
SELECT DISTINCT movie_id, title, genres
FROM movies_stage
ON CONFLICT (movie_id) DO NOTHING;

-- B) Load new ratings
INSERT INTO ratings (user_id, movie_id, rating, ts)
SELECT
  user_id,
  movie_id,
  rating,
  to_timestamp(ts) AS ts
FROM ratings_stage
ON CONFLICT (user_id, movie_id, ts) DO NOTHING;

-- C) Load new tags
INSERT INTO tags (user_id, movie_id, tag, ts)
SELECT
  user_id,
  movie_id,
  tag,
  to_timestamp(ts) AS ts
FROM tags_stage
ON CONFLICT (user_id, movie_id, tag, ts) DO NOTHING;

COMMIT;