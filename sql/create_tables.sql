-- staging tables already created by your load script
-- now define your production tables:

CREATE TABLE IF NOT EXISTS movies (
  movie_id   INTEGER PRIMARY KEY,
  title      TEXT,
  genres     TEXT
);

CREATE TABLE IF NOT EXISTS ratings (
  user_id    INTEGER,
  movie_id   INTEGER,
  rating     NUMERIC,
  ts         TIMESTAMP,
  PRIMARY KEY (user_id, movie_id, ts)
);

CREATE TABLE IF NOT EXISTS tags (
  user_id    INTEGER,
  movie_id   INTEGER,
  tag        TEXT,
  ts         TIMESTAMP,
  PRIMARY KEY (user_id, movie_id, tag, ts)
);

-- 2) staging tables
CREATE TABLE IF NOT EXISTS movies_stage (
  movie_id   INTEGER,
  title      TEXT,
  genres     TEXT
);

CREATE TABLE IF NOT EXISTS ratings_stage (
  user_id    INTEGER,
  movie_id   INTEGER,
  rating     NUMERIC,
  ts         INTEGER
);

CREATE TABLE IF NOT EXISTS tags_stage (
  user_id    INTEGER,
  movie_id   INTEGER,
  tag        TEXT,
  ts         INTEGER
);