-- db_setup.sql
-- 
-- SQL script to create the players table in PostgreSQL

CREATE TABLE IF NOT EXISTS players (
    player_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    position VARCHAR(50),
    jersey_number INTEGER,
    date_of_birth DATE,
    nationality VARCHAR(100),
    height INTEGER,
    weight INTEGER,
    shoots VARCHAR(10),
    current_team_id BIGINT,
    current_team_name VARCHAR(100),
    current_team_abbreviation VARCHAR(10),
    is_active BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Optional: Create an index on last_name for faster searches
CREATE INDEX IF NOT EXISTS idx_players_last_name ON players(last_name);