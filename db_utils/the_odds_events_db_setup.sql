-- create_the_odds_events.sql
--
-- SQL script to create the game_info table in the the_odds PostgreSQL database

CREATE TABLE IF NOT EXISTS game_info (
    id VARCHAR(255) PRIMARY KEY,
    sport_key VARCHAR(100) NOT NULL,
    sport_title VARCHAR(100) NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    commence_time TIMESTAMP WITH TIME ZONE NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create an index on commence_time for efficient querying by date
CREATE INDEX IF NOT EXISTS idx_game_info_commence_time ON game_info(commence_time); 