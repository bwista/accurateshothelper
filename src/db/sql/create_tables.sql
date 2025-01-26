-- Create NHL Players Database Tables
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200) NOT NULL,
    position VARCHAR(50),
    jersey_number INTEGER,
    date_of_birth DATE,
    nationality VARCHAR(100),
    height INTEGER,  -- in inches
    weight INTEGER,  -- in pounds
    shoots VARCHAR(10),
    current_team_id INTEGER,
    current_team_name VARCHAR(100),
    current_team_abbreviation VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for players table
CREATE INDEX IF NOT EXISTS idx_players_full_name ON players(full_name);
CREATE INDEX IF NOT EXISTS idx_players_current_team ON players(current_team_id);

-- Create The Odds Database Tables

-- Game Information Table
CREATE TABLE IF NOT EXISTS game_info (
    id VARCHAR(50) PRIMARY KEY,
    sport_key VARCHAR(50) NOT NULL,
    sport_title VARCHAR(50) NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    commence_time TIMESTAMP WITH TIME ZONE NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for game_info table
CREATE INDEX IF NOT EXISTS idx_game_info_commence_time ON game_info(commence_time);
CREATE INDEX IF NOT EXISTS idx_game_info_teams ON game_info(home_team, away_team);

-- Moneyline Odds Table
CREATE TABLE IF NOT EXISTS moneyline_odds (
    game_id VARCHAR(50) NOT NULL,
    sportsbook VARCHAR(50) NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    price INTEGER NOT NULL,
    last_update TIMESTAMP WITH TIME ZONE NOT NULL,
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (game_id, sportsbook, team_name),
    FOREIGN KEY (game_id) REFERENCES game_info(id)
);

-- Create indices for moneyline_odds table
CREATE INDEX IF NOT EXISTS idx_moneyline_odds_game ON moneyline_odds(game_id);
CREATE INDEX IF NOT EXISTS idx_moneyline_odds_sportsbook ON moneyline_odds(sportsbook);
CREATE INDEX IF NOT EXISTS idx_moneyline_odds_last_update ON moneyline_odds(last_update);

-- Player Shots on Goal Odds Table
CREATE TABLE IF NOT EXISTS player_sog_odds (
    game_id VARCHAR(50) NOT NULL,
    sportsbook VARCHAR(50) NOT NULL,
    player_name VARCHAR(200) NOT NULL,
    market_type VARCHAR(50) NOT NULL,
    handicap NUMERIC(4,1) NOT NULL,
    price INTEGER NOT NULL,
    last_update TIMESTAMP WITH TIME ZONE NOT NULL,
    scraped_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (game_id, sportsbook, player_name, market_type, handicap),
    FOREIGN KEY (game_id) REFERENCES game_info(id)
);

-- Create indices for player_sog_odds table
CREATE INDEX IF NOT EXISTS idx_player_sog_odds_game ON player_sog_odds(game_id);
CREATE INDEX IF NOT EXISTS idx_player_sog_odds_player ON player_sog_odds(player_name);
CREATE INDEX IF NOT EXISTS idx_player_sog_odds_sportsbook ON player_sog_odds(sportsbook);
CREATE INDEX IF NOT EXISTS idx_player_sog_odds_last_update ON player_sog_odds(last_update);

-- Add comments to tables and columns
COMMENT ON TABLE players IS 'NHL player information and current team details';
COMMENT ON TABLE game_info IS 'Basic game information from The Odds API';
COMMENT ON TABLE moneyline_odds IS 'Moneyline (head-to-head) betting odds for NHL games';
COMMENT ON TABLE player_sog_odds IS 'Player shots on goal betting odds for NHL games'; 