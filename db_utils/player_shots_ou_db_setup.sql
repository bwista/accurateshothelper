-- Drop the table if it already exists
DROP TABLE IF EXISTS player_shots_over_under;

-- Create the table
CREATE TABLE player_shots_ou (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(32),
    sportsbook VARCHAR(50),
    player VARCHAR(100),
    ou VARCHAR(10),
    handicap FLOAT,
    odds INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE,
    UNIQUE(game_id, sportsbook, player, ou, odds, handicap, timestamp)
);