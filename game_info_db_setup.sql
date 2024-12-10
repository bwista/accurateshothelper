-- create_game_info.sql
--
-- SQL script to create the game_info table in the prop_odds PostgreSQL database

CREATE TABLE IF NOT EXISTS game_info (
    id BIGINT PRIMARY KEY,
    game_id VARCHAR(255) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    participants JSONB DEFAULT '[]'::jsonb,
    is_started BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Optional: Create an index on game_id for faster lookups
-- CREATE INDEX IF NOT EXISTS idx_game_info_game_id ON game_info(game_id);

-- Optional: Create an index on start_timestamp for efficient querying by date
-- CREATE INDEX IF NOT EXISTS idx_game_info_start_timestamp ON game_info(start_timestamp);