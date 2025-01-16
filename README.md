# Accurate Shot Helper

Accurate Shot Helper is a comprehensive Python-based tool designed to process, analyze, and predict hockey player performance metrics. Leveraging various data sources and statistical models, it provides insights into player statistics, lineup efficiencies, and predictive analytics to aid in decision-making for teams, analysts, and enthusiasts.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Data Scraping:** Utilize `nst_scraper` to fetch the latest player and team statistics
- **Data Processing:** Clean and organize data using Pandas and custom utility functions
- **Statistical Modeling:** Implement regression models to predict performance metrics like Goals Per Minute (GPM) and Expected Goals (xG)
- **Lineup Analysis:** Analyze and optimize team lineups based on player statistics and predictive models
- **Database Integration:** Store and retrieve data efficiently using PostgreSQL
- **Visualization:** Generate insightful plots and figures with Matplotlib and Seaborn

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/yourusername/accurateshothelper.git
   cd accurateshothelper
   ```

2. **Create a Virtual Environment**

   ```bash
   python -m venv .venv
   # On Windows:
   .venv\Scripts\activate
   # On Unix/MacOS:
   source .venv/bin/activate
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up Environment Variables**

   Create a `.env` file in the root directory and add your database configurations:

   ```env
   NHL_DB_NAME=your_db_name
   NHL_DB_USER=your_db_user
   NHL_DB_PASSWORD=your_db_password
   NHL_DB_HOST=your_db_host
   NHL_DB_PORT=your_db_port
   ```

## Usage

### Notebooks

The project includes several Jupyter notebooks for different analyses:

- `notebooks/x_shots_model_03.ipynb` - Latest shots model and analysis
- `notebooks/nst_scraper.ipynb` - Data scraping implementation
- `notebooks/ixg60_calc.ipynb` - Expected goals calculations
- `notebooks/xgm_model_01.ipynb` - Expected goals model
- `notebooks/sog_corr_calc.ipynb` - Shots on goal correlation analysis

### Core Modules

- `src/data_processing/` - Data processing utilities and functions
  - `nst_scraper.py` - Natural Stat Trick scraping functionality
  - `team_utils.py` - Team-related data processing
  - `game_utils.py` - Game data processing
  - `wager_utils.py` - Wager analysis utilities

- `src/db/` - Database interaction modules
  - `nhl_db_utils.py` - NHL database utilities
  - `the_odds_db_utils.py` - Odds database interactions

- `src/entities/` - Core entity definitions
  - `lineup.py` - Team lineup management

## Project Structure

```
accurateshothelper/
├── notebooks/           # Jupyter notebooks for analysis
├── src/                # Source code
│   ├── data_processing/ # Data processing modules
│   ├── db/             # Database utilities
│   └── entities/       # Core entity definitions
├── requirements.txt    # Project dependencies
├── .env               # Environment variables
└── README.md          # Project documentation
```

## Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b feature/improvement`)
3. Make your changes
4. Commit your changes (`git commit -am 'Add new feature'`)
5. Push to the branch (`git push origin feature/improvement`)
6. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 