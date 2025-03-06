-- Create NHL Players Database Tables
CREATE TABLE IF NOT EXISTS public.players (
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

-- Create NHL Game Tables

-- Games Table
CREATE TABLE IF NOT EXISTS public.games (
    game_id INTEGER PRIMARY KEY,
    season INTEGER NOT NULL,
    game_type INTEGER NOT NULL,
    game_date DATE NOT NULL,
    venue_name VARCHAR(100),
    venue_location VARCHAR(100),
    start_time_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    eastern_utc_offset VARCHAR(6),
    venue_utc_offset VARCHAR(6),
    game_state VARCHAR(20),
    game_schedule_state VARCHAR(20),
    period_number INTEGER,
    period_type VARCHAR(10),
    max_regulation_periods INTEGER,
    reg_periods INTEGER,
    last_period_type VARCHAR(10),
    ot_periods INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for games table
CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);

-- Game Teams Table
CREATE TABLE IF NOT EXISTS public.game_teams (
    game_team_id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES games(game_id),
    team_id INTEGER NOT NULL,
    team_name VARCHAR(100),
    team_abbrev VARCHAR(10),
    is_home_team BOOLEAN NOT NULL,
    score INTEGER,
    shots_on_goal INTEGER,
    team_logo_url VARCHAR(255),
    team_dark_logo_url VARCHAR(255),
    place_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, team_id)
);

-- Create indices for game teams table
CREATE INDEX IF NOT EXISTS idx_game_teams_game ON game_teams(game_id);
CREATE INDEX IF NOT EXISTS idx_game_teams_team ON game_teams(team_id);

-- Game Broadcasts Table
CREATE TABLE IF NOT EXISTS public.game_broadcasts (
    broadcast_id INTEGER PRIMARY KEY,
    game_id INTEGER REFERENCES games(game_id),
    market VARCHAR(10),
    country_code VARCHAR(2),
    network VARCHAR(50),
    sequence_number INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for game broadcasts table
CREATE INDEX IF NOT EXISTS idx_game_broadcasts_game ON game_broadcasts(game_id);

-- Player Game Statistics Table
CREATE TABLE IF NOT EXISTS public.player_game_stats (
    player_game_stat_id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES games(game_id),
    game_team_id INTEGER REFERENCES game_teams(game_team_id),
    player_id INTEGER REFERENCES players(player_id),
    sweater_number INTEGER,
    player_name VARCHAR(100),
    position VARCHAR(2),
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    plus_minus INTEGER DEFAULT 0,
    pim INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0,
    power_play_goals INTEGER DEFAULT 0,
    shots_on_goal INTEGER DEFAULT 0,
    faceoff_winning_pctg DECIMAL(5,3),
    time_on_ice INTERVAL,
    blocked_shots INTEGER DEFAULT 0,
    shifts INTEGER DEFAULT 0,
    giveaways INTEGER DEFAULT 0,
    takeaways INTEGER DEFAULT 0,
    is_starter BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, player_id)
);

-- Create indices for player game stats table
CREATE INDEX IF NOT EXISTS idx_player_game_stats_game ON player_game_stats(game_id);
CREATE INDEX IF NOT EXISTS idx_player_game_stats_player ON player_game_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_game_stats_team ON player_game_stats(game_team_id);

-- Goalie Game Statistics Table
CREATE TABLE IF NOT EXISTS public.goalie_game_stats (
    goalie_game_stat_id SERIAL PRIMARY KEY,
    player_game_stat_id INTEGER REFERENCES player_game_stats(player_game_stat_id),
    even_strength_shots_against VARCHAR(20),
    power_play_shots_against VARCHAR(20),
    shorthanded_shots_against VARCHAR(20),
    save_shots_against VARCHAR(20),
    save_percentage DECIMAL(5,3),
    even_strength_goals_against INTEGER DEFAULT 0,
    power_play_goals_against INTEGER DEFAULT 0,
    shorthanded_goals_against INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    shots_against INTEGER DEFAULT 0,
    saves INTEGER DEFAULT 0,
    decision VARCHAR(1),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for goalie game stats table
CREATE INDEX IF NOT EXISTS idx_goalie_game_stats_player ON goalie_game_stats(player_game_stat_id);

-- Game Clock Status Table
CREATE TABLE IF NOT EXISTS public.game_clock_status (
    game_clock_id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES games(game_id),
    time_remaining VARCHAR(10),
    seconds_remaining INTEGER,
    is_running BOOLEAN,
    is_intermission BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for game clock status table
CREATE INDEX IF NOT EXISTS idx_game_clock_status_game ON game_clock_status(game_id);

-- Add comments to game tables
COMMENT ON TABLE public.games IS 'NHL game information including venue and timing details';
COMMENT ON TABLE public.game_teams IS 'Team-specific information for each NHL game';
COMMENT ON TABLE public.game_broadcasts IS 'Broadcast information for NHL games';
COMMENT ON TABLE public.player_game_stats IS 'Player statistics for each NHL game';
COMMENT ON TABLE public.goalie_game_stats IS 'Detailed goalie statistics for each NHL game';
COMMENT ON TABLE public.game_clock_status IS 'Game clock and period information for NHL games';

-- Create The Odds Database Tables

-- Game Information Table
CREATE TABLE IF NOT EXISTS public.game_info (
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
CREATE TABLE IF NOT EXISTS public.moneyline_odds (
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
CREATE TABLE IF NOT EXISTS public.player_sog_odds (
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

-- Player Saves Odds Table
CREATE TABLE IF NOT EXISTS public.player_saves_odds (
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

-- Create indices for player_saves_odds table
CREATE INDEX IF NOT EXISTS idx_player_saves_odds_game ON player_saves_odds(game_id);
CREATE INDEX IF NOT EXISTS idx_player_saves_odds_player ON player_saves_odds(player_name);
CREATE INDEX IF NOT EXISTS idx_player_saves_odds_sportsbook ON player_saves_odds(sportsbook);
CREATE INDEX IF NOT EXISTS idx_player_saves_odds_last_update ON player_saves_odds(last_update);

-- Create Prop Odds Database Tables

-- Game Information Table for Prop Odds
CREATE TABLE IF NOT EXISTS public.prop_game_info (
    id VARCHAR(50) PRIMARY KEY,
    game_id VARCHAR(50) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for prop_game_info table
CREATE INDEX IF NOT EXISTS idx_prop_game_info_game_id ON prop_game_info(game_id);
CREATE INDEX IF NOT EXISTS idx_prop_game_info_teams ON prop_game_info(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_prop_game_info_start ON prop_game_info(start_timestamp);

-- Player Shots Over/Under Table
CREATE TABLE IF NOT EXISTS public.player_shots_ou (
    game_id VARCHAR(50) NOT NULL,
    sportsbook VARCHAR(50) NOT NULL,
    player VARCHAR(200) NOT NULL,
    ou VARCHAR(10) NOT NULL,
    handicap NUMERIC(4,1) NOT NULL,
    odds INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (game_id, sportsbook, player, ou, handicap, odds, timestamp)
);

-- Create indices for player_shots_ou table
CREATE INDEX IF NOT EXISTS idx_player_shots_ou_game ON player_shots_ou(game_id);
CREATE INDEX IF NOT EXISTS idx_player_shots_ou_player ON player_shots_ou(player);
CREATE INDEX IF NOT EXISTS idx_player_shots_ou_timestamp ON player_shots_ou(timestamp);

-- Create Natural Stat Trick Database Tables

-- 5v5 Goalie Stats Table
CREATE TABLE IF NOT EXISTS public.goalie_stats_5v5 (
    player character varying(200) NOT NULL,
    team character varying(10) NOT NULL,
    gp integer,
    toi numeric,
    shots_against integer,
    saves integer,
    goals_against integer,
    sv_pct numeric,
    gaa numeric,
    gsaa numeric,
    xg_against numeric,
    hd_shots_against integer,
    hd_saves integer,
    hd_goals_against integer,
    hdsv_pct numeric,
    hdgaa numeric,
    hdgsaa numeric,
    md_shots_against integer,
    md_saves integer,
    md_goals_against integer,
    mdsv_pct numeric,
    mdgaa numeric,
    mdgsaa numeric,
    ld_shots_against integer,
    ld_saves integer,
    ld_goals_against integer,
    ldsv_pct numeric,
    ldgaa numeric,
    ldgsaa numeric,
    rush_attempts_against integer,
    rebound_attempts_against integer,
    avg_shot_distance numeric,
    avg_goal_distance numeric,
    date date NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    season integer,
    CONSTRAINT goalie_stats_5v5_pkey PRIMARY KEY (player, date)
);

-- All Situations Goalie Stats Table
CREATE TABLE IF NOT EXISTS public.goalie_stats_all (
    player character varying(200) NOT NULL,
    team character varying(10) NOT NULL,
    gp integer,
    toi numeric,
    shots_against integer,
    saves integer,
    goals_against integer,
    sv_pct numeric,
    gaa numeric,
    gsaa numeric,
    xg_against numeric,
    hd_shots_against integer,
    hd_saves integer,
    hd_goals_against integer,
    hdsv_pct numeric,
    hdgaa numeric,
    hdgsaa numeric,
    md_shots_against integer,
    md_saves integer,
    md_goals_against integer,
    mdsv_pct numeric,
    mdgaa numeric,
    mdgsaa numeric,
    ld_shots_against integer,
    ld_saves integer,
    ld_goals_against integer,
    ldsv_pct numeric,
    ldgaa numeric,
    ldgsaa numeric,
    rush_attempts_against integer,
    rebound_attempts_against integer,
    avg_shot_distance numeric,
    avg_goal_distance numeric,
    date date NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    season integer,
    CONSTRAINT goalie_stats_all_pkey PRIMARY KEY (player, date)
);

-- Penalty Kill Goalie Stats Table
CREATE TABLE IF NOT EXISTS public.goalie_stats_pk (
    player character varying(200) NOT NULL,
    team character varying(10) NOT NULL,
    gp integer,
    toi numeric,
    shots_against integer,
    saves integer,
    goals_against integer,
    sv_pct numeric,
    gaa numeric,
    gsaa numeric,
    xg_against numeric,
    hd_shots_against integer,
    hd_saves integer,
    hd_goals_against integer,
    hdsv_pct numeric,
    hdgaa numeric,
    hdgsaa numeric,
    md_shots_against integer,
    md_saves integer,
    md_goals_against integer,
    mdsv_pct numeric,
    mdgaa numeric,
    mdgsaa numeric,
    ld_shots_against integer,
    ld_saves integer,
    ld_goals_against integer,
    ldsv_pct numeric,
    ldgaa numeric,
    ldgsaa numeric,
    rush_attempts_against integer,
    rebound_attempts_against integer,
    avg_shot_distance numeric,
    avg_goal_distance numeric,
    date date NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    season integer,
    CONSTRAINT goalie_stats_pk_pkey PRIMARY KEY (player, date)
);

-- Create indices for goalie stats tables
CREATE INDEX IF NOT EXISTS idx_goalie_stats_5v5_player ON goalie_stats_5v5(player);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_5v5_date ON goalie_stats_5v5(date);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_5v5_team ON goalie_stats_5v5(team);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_5v5_season ON goalie_stats_5v5(season);

CREATE INDEX IF NOT EXISTS idx_goalie_stats_all_player ON goalie_stats_all(player);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_all_date ON goalie_stats_all(date);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_all_team ON goalie_stats_all(team);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_all_season ON goalie_stats_all(season);

CREATE INDEX IF NOT EXISTS idx_goalie_stats_pk_player ON goalie_stats_pk(player);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_pk_date ON goalie_stats_pk(date);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_pk_team ON goalie_stats_pk(team);
CREATE INDEX IF NOT EXISTS idx_goalie_stats_pk_season ON goalie_stats_pk(season);

-- Add comments to tables and columns
COMMENT ON TABLE public.players IS 'NHL player information and current team details';
COMMENT ON TABLE public.game_info IS 'Basic game information from The Odds API';
COMMENT ON TABLE public.moneyline_odds IS 'Moneyline (head-to-head) betting odds for NHL games';
COMMENT ON TABLE public.player_sog_odds IS 'Player shots on goal betting odds for NHL games';
COMMENT ON TABLE public.player_saves_odds IS 'Player saves betting odds for NHL games';
COMMENT ON TABLE public.prop_game_info IS 'Game information for prop betting odds';
COMMENT ON TABLE public.player_shots_ou IS 'Player shots over/under prop betting odds';
COMMENT ON TABLE public.goalie_stats_5v5 IS 'NHL goalie statistics at 5v5 play including high, medium, and low danger metrics';
COMMENT ON TABLE public.goalie_stats_all IS 'NHL goalie statistics for all game situations including high, medium, and low danger metrics';
COMMENT ON TABLE public.goalie_stats_pk IS 'NHL goalie statistics during penalty kill situations including high, medium, and low danger metrics';

-- Create Team Stats Tables

-- 5v5 Team Stats Table
CREATE TABLE IF NOT EXISTS public.team_stats_5v5 (
    team character varying(50) NOT NULL,
    gp integer,
    toi numeric,
    w integer,
    l integer,
    otl integer,
    row integer,
    points integer,
    point_pct numeric,
    cf integer,
    ca integer,
    cf_pct numeric,
    ff integer,
    fa integer,
    ff_pct numeric,
    sf integer,
    sa integer,
    sf_pct numeric,
    gf integer,
    ga integer,
    gf_pct numeric,
    xgf numeric,
    xga numeric,
    xgf_pct numeric,
    scf integer,
    sca integer,
    scf_pct numeric,
    scsf integer,
    scsa integer,
    scsf_pct numeric,
    scgf integer,
    scga integer,
    scgf_pct numeric,
    scsh_pct numeric,
    scsv_pct numeric,
    hdcf integer,
    hdca integer,
    hdcf_pct numeric,
    hdsf integer,
    hdsa integer,
    hdsf_pct numeric,
    hdgf integer,
    hdga integer,
    hdgf_pct numeric,
    hdsh_pct numeric,
    hdsv_pct numeric,
    mdcf integer,
    mdca integer,
    mdcf_pct numeric,
    mdsf integer,
    mdsa integer,
    mdsf_pct numeric,
    mdgf integer,
    mdga integer,
    mdgf_pct numeric,
    mdsh_pct numeric,
    mdsv_pct numeric,
    ldcf integer,
    ldca integer,
    ldcf_pct numeric,
    ldsf integer,
    ldsa integer,
    ldsf_pct numeric,
    ldgf integer,
    ldga integer,
    ldgf_pct numeric,
    ldsh_pct numeric,
    ldsv_pct numeric,
    sh_pct numeric,
    sv_pct numeric,
    pdo numeric,
    date date NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    season integer,
    CONSTRAINT team_stats_5v5_pkey PRIMARY KEY (team, date)
);

-- All Situations Team Stats Table
CREATE TABLE IF NOT EXISTS public.team_stats_all (
    team character varying(50) NOT NULL,
    gp integer,
    toi numeric,
    w integer,
    l integer,
    otl integer,
    row integer,
    points integer,
    point_pct numeric,
    cf integer,
    ca integer,
    cf_pct numeric,
    ff integer,
    fa integer,
    ff_pct numeric,
    sf integer,
    sa integer,
    sf_pct numeric,
    gf integer,
    ga integer,
    gf_pct numeric,
    xgf numeric,
    xga numeric,
    xgf_pct numeric,
    scf integer,
    sca integer,
    scf_pct numeric,
    scsf integer,
    scsa integer,
    scsf_pct numeric,
    scgf integer,
    scga integer,
    scgf_pct numeric,
    scsh_pct numeric,
    scsv_pct numeric,
    hdcf integer,
    hdca integer,
    hdcf_pct numeric,
    hdsf integer,
    hdsa integer,
    hdsf_pct numeric,
    hdgf integer,
    hdga integer,
    hdgf_pct numeric,
    hdsh_pct numeric,
    hdsv_pct numeric,
    mdcf integer,
    mdca integer,
    mdcf_pct numeric,
    mdsf integer,
    mdsa integer,
    mdsf_pct numeric,
    mdgf integer,
    mdga integer,
    mdgf_pct numeric,
    mdsh_pct numeric,
    mdsv_pct numeric,
    ldcf integer,
    ldca integer,
    ldcf_pct numeric,
    ldsf integer,
    ldsa integer,
    ldsf_pct numeric,
    ldgf integer,
    ldga integer,
    ldgf_pct numeric,
    ldsh_pct numeric,
    ldsv_pct numeric,
    sh_pct numeric,
    sv_pct numeric,
    pdo numeric,
    ppgf integer,
    ppga integer,
    pp_pct numeric,
    pkgf integer,
    pkga integer,
    pk_pct numeric,
    date date NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    season integer,
    CONSTRAINT team_stats_all_pkey PRIMARY KEY (team, date)
);

-- Penalty Kill Team Stats Table
CREATE TABLE IF NOT EXISTS public.team_stats_pk (
    team character varying(50) NOT NULL,
    gp integer,
    toi numeric,
    w integer,
    l integer,
    otl integer,
    row integer,
    points integer,
    point_pct numeric,
    cf integer,
    ca integer,
    cf_pct numeric,
    ff integer,
    fa integer,
    ff_pct numeric,
    sf integer,
    sa integer,
    sf_pct numeric,
    gf integer,
    ga integer,
    gf_pct numeric,
    xgf numeric,
    xga numeric,
    xgf_pct numeric,
    scf integer,
    sca integer,
    scf_pct numeric,
    scsf integer,
    scsa integer,
    scsf_pct numeric,
    scgf integer,
    scga integer,
    scgf_pct numeric,
    scsh_pct numeric,
    scsv_pct numeric,
    hdcf integer,
    hdca integer,
    hdcf_pct numeric,
    hdsf integer,
    hdsa integer,
    hdsf_pct numeric,
    hdgf integer,
    hdga integer,
    hdgf_pct numeric,
    hdsh_pct numeric,
    hdsv_pct numeric,
    mdcf integer,
    mdca integer,
    mdcf_pct numeric,
    mdsf integer,
    mdsa integer,
    mdsf_pct numeric,
    mdgf integer,
    mdga integer,
    mdgf_pct numeric,
    mdsh_pct numeric,
    mdsv_pct numeric,
    ldcf integer,
    ldca integer,
    ldcf_pct numeric,
    ldsf integer,
    ldsa integer,
    ldsf_pct numeric,
    ldgf integer,
    ldga integer,
    ldgf_pct numeric,
    ldsh_pct numeric,
    ldsv_pct numeric,
    sh_pct numeric,
    sv_pct numeric,
    pdo numeric,
    date date NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    season integer,
    CONSTRAINT team_stats_pk_pkey PRIMARY KEY (team, date)
);

-- Power Play Team Stats Table
CREATE TABLE IF NOT EXISTS public.team_stats_pp (
    team character varying(50) NOT NULL,
    gp integer,
    toi numeric,
    w integer,
    l integer,
    otl integer,
    row integer,
    points integer,
    point_pct numeric,
    cf integer,
    ca integer,
    cf_pct numeric,
    ff integer,
    fa integer,
    ff_pct numeric,
    sf integer,
    sa integer,
    sf_pct numeric,
    gf integer,
    ga integer,
    gf_pct numeric,
    xgf numeric,
    xga numeric,
    xgf_pct numeric,
    scf integer,
    sca integer,
    scf_pct numeric,
    scsf integer,
    scsa integer,
    scsf_pct numeric,
    scgf integer,
    scga integer,
    scgf_pct numeric,
    scsh_pct numeric,
    scsv_pct numeric,
    hdcf integer,
    hdca integer,
    hdcf_pct numeric,
    hdsf integer,
    hdsa integer,
    hdsf_pct numeric,
    hdgf integer,
    hdga integer,
    hdgf_pct numeric,
    hdsh_pct numeric,
    hdsv_pct numeric,
    mdcf integer,
    mdca integer,
    mdcf_pct numeric,
    mdsf integer,
    mdsa integer,
    mdsf_pct numeric,
    mdgf integer,
    mdga integer,
    mdgf_pct numeric,
    mdsh_pct numeric,
    mdsv_pct numeric,
    ldcf integer,
    ldca integer,
    ldcf_pct numeric,
    ldsf integer,
    ldsa integer,
    ldsf_pct numeric,
    ldgf integer,
    ldga integer,
    ldgf_pct numeric,
    ldsh_pct numeric,
    ldsv_pct numeric,
    sh_pct numeric,
    sv_pct numeric,
    pdo numeric,
    date date NOT NULL,
    last_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    season integer,
    CONSTRAINT team_stats_pp_pkey PRIMARY KEY (team, date)
);

-- Create indices for team stats tables
CREATE INDEX IF NOT EXISTS idx_team_stats_5v5_team ON team_stats_5v5(team);
CREATE INDEX IF NOT EXISTS idx_team_stats_5v5_date ON team_stats_5v5(date);
CREATE INDEX IF NOT EXISTS idx_team_stats_5v5_season ON team_stats_5v5(season);

CREATE INDEX IF NOT EXISTS idx_team_stats_all_team ON team_stats_all(team);
CREATE INDEX IF NOT EXISTS idx_team_stats_all_date ON team_stats_all(date);
CREATE INDEX IF NOT EXISTS idx_team_stats_all_season ON team_stats_all(season);

CREATE INDEX IF NOT EXISTS idx_team_stats_pk_team ON team_stats_pk(team);
CREATE INDEX IF NOT EXISTS idx_team_stats_pk_date ON team_stats_pk(date);
CREATE INDEX IF NOT EXISTS idx_team_stats_pk_season ON team_stats_pk(season);

CREATE INDEX IF NOT EXISTS idx_team_stats_pp_team ON team_stats_pp(team);
CREATE INDEX IF NOT EXISTS idx_team_stats_pp_date ON team_stats_pp(date);
CREATE INDEX IF NOT EXISTS idx_team_stats_pp_season ON team_stats_pp(season);

-- Add comments to team stats tables
COMMENT ON TABLE public.team_stats_5v5 IS 'NHL team statistics at 5v5 play including possession, shot, and goal metrics';
COMMENT ON TABLE public.team_stats_all IS 'NHL team statistics for all game situations including power play and penalty kill metrics';
COMMENT ON TABLE public.team_stats_pk IS 'NHL team statistics during penalty kill situations';
COMMENT ON TABLE public.team_stats_pp IS 'NHL team statistics during power play situations'; 