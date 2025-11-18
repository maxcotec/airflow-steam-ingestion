# Steam Game Data Ingestion Pipeline

A comprehensive Apache Airflow 3 data pipeline for ingesting and analyzing Steam game data. The pipeline collects trending game information, player counts, and popularity statistics hourly and stores them in a local MySQL database.

Youtube tutorial Link: https://youtu.be/fuqF60beLdk?si=N8f4nlY34WZnqEmd
![youtube video.png](youtube%20video.png)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Airflow 3 DAG                         │
│              (Runs Hourly - steam_ingestion_pipeline)           │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────▼──────────┐ 
                    │    Task 1.         │  
                    │    Top 100 Games   │  
                    └─────────┬──────────┘  
                              │                 
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼─────┐        ┌──────▼──────┐      ┌───────▼─────┐
   │ Task 1.  │        │   Task 2.   │      │  Task 3.    │
   │ Trending │        │   Game      │      │  Popularity │
   │ Games    │        │   Details   │      │  stats      │
   └────┬─────┘        └───────┬─────┘      └────────┬────┘
        │                      │                     │
        └──────────────────────┼─────────────────────┘
                               │
                      ┌────────▼──────────┐
                      │     Task 5        │
                      │  Merge & Clean    │
                      │   - Transform     │
                      │   - Aggregate     │
                      │   - Deduplicate   │
                      └─────────┬─────────┘
                                │
                      ┌─────────▼──────────┐
                      │  MySQL Database    │
                      │ steam_games        │
                      │ ├─ trending_games  │
                      │ ├─ game_catalog    │
                      │ ├─ player_count    │
                      │ ├─ popularity_stats│
                      │ └─ games_cleaned   │
                      └────────────────────┘
```
## About the Code (Generated with Cline AI)
This project was generated using [Cline](https://cline.bot/) — an AI coding agent.
The [full prompt](Cline_Prompt.docx) used for generation is included in this repository. 

Because AI models are non-deterministic, running the same prompt again may produce different code structures, approaches, or implementations. 
Results also vary based on the LLM provider.

For transparency, this project was built using:
```shell
Provider: Cline API
Plan Model: x-ai/grok-code-fast-1
Act Model: anthropic/claude-haiku-4.5
```
Despite variations, the generated code provides a solid blueprint for building the full workflow end-to-end.

## Pipeline Tasks

### Task 1: Fetch Top Trending Games
- **Endpoint**: SteamSpy API (`top100in2weeks`)
- **Output**: `trending_games` table
- **Data**: appid, name, median_2weeks, run_date, run_hour
- **Deduplication**: Unique constraint on (appid, run_date, run_hour)

### Task 2: Fetch Game Details
- **Endpoint**: Steam Store API (`appdetails`)
- **Output**: `game_catalog` table (static, no time dimensions)
- **Optimization**: Skips already-cataloged games to minimize API calls
- **Data**: name, developer, release_date, genres, price, description, platforms

### Task 3: Fetch Player Count
- **Endpoint**: Steam API (`GetNumberOfCurrentPlayers`)
- **Output**: `player_count` table
- **Data**: current_players, run_date, run_hour

### Task 4: Fetch Popularity Stats
- **Endpoint**: SteamSpy API (`appdetails`)
- **Output**: `popularity_stats` table
- **Data**: owners, ccu, reviews (positive/negative), playtime (average/median), price, tags

### Task 5: Merge and Clean
- **Operation**: Data transformation and aggregation
- **Inputs**: All raw tables
- **Output**: `games_cleaned` table (analytics-ready silver layer)
- **Transformations**:
  - Parse owners string to integer (extract lower bound in millions)
  - Convert price from cents to USD
  - Standardize numeric fields
  - Drop records with missing appid/name
  - Create hourly KPI snapshots per game

## Project Structure

```
airflow-steam-ingestion/
├── dags/
│   └── steam_ingestion_dag.py          # Main DAG with 5 tasks
├── src/
│   ├── __init__.py
│   ├── database.py                     # MySQL connection & table management
│   ├── steam_api.py                    # Steam/SteamSpy API clients
│   ├── data_processing.py              # Data cleaning & aggregation
│   └── utils.py                        # Date/time utilities
├── airflow.cfg                         # Airflow configuration (auto-generated)
├── requirements.txt                    # Python dependencies
├── init_db.py                          # Database initialization script
├── README.md                           # This file
└── README_DATABASE.md                  # Database schema documentation
```

## Installation & Setup

### Prerequisites
- Python 3.10+
- MySQL Server running locally (port 3306)
- pip package manager

### Step 1: Install Dependencies

```bash
# install and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install required Python packages
pip install -r requirements.txt
```

### Step 2: Initialize Airflow

```bash
# create airflow home directory
mkdir airflow_home

# set airflow home to current directory (Mac/Linux)
export AIRFLOW_HOME=$(pwd)/airflow_home

# Windows (PowerShell)
$env:AIRFLOW_HOME = "$(Get-Location)\airflow_home"

# Windows (Command Prompt)
set AIRFLOW_HOME=%cd%\airflow_home

# finally run airflow
airflow standalone
```

This command will:
- Create an SQLite metadata database at `airflow-steam-ingestion/airflow_home/airflow.db`
- Generate default `airflow.cfg` configuration
- Start the Airflow API server on http://localhost:8080
- Start the scheduler

Upon starting airflow first time, it should populate default `airflow.cfg` under `airflow-steam-ingestion/airflow_home/`

### Step 3: Load Dag file
First, stop `airflow standalone` process.
Set `dag_folder` under `airflow-steam-ingestion/airflow_home/airflow.cfg` to absolute path of your dags directory
e.g. `/Users/<user_name>/airflow-steam-ingestion/dags`

Ensure the path in `steam_ingestion_dag.py` is correct for your environment:
```python
sys.path.insert(0, '/Users/<user_name>/airflow-steam-ingestion')
```

### Step 4: Remove example dags (optional)
set `load_examples = False` under `airflow-steam-ingestion/airflow_home/airflow.cfg`.

Since example dags are already loaded into database, so you need to clear airflow db now
`airflow db reset` 

CAUTION: This removes everything from airflow db including dag and task run records. 
If you only want to delete example dags; 
```bash
airflow db shell
DELETE FROM dag WHERE dag_id LIKE 'example_%' OR dag_id LIKE 'tutorial_%';
```

### Step 5: Create MySQL Database

Ensure MySQL is running on localhost. Then initialize the database and tables:

```bash
# From the project root directory
python init_db.py
```

This script will:
1. Create `steam_games` database (if not exists)
2. Create all 5 required tables with proper indexes

**Verify connection credentials**:
- Host: `localhost`
- User: `root`
- Password: (none/empty)
- Database: `steam_games`

If you need different credentials, update the `MySQLConnector` initialization in the DAG and scripts.

### Step 6: Run airflow 
```shell
airflow standalone
```

### Step 7: Analytics Insights
Once you have ingested sufficient data using this DAG (Brown and Silver stages), you can visualize and analyze it using the dashboards in the [steam-games-analytics-dashboards](https://github.com/maxcotec/steam-games-analytics-dashboards) repository.


## Running the Pipeline

### Manual DAG Trigger (Testing)

1. Open Airflow UI: http://localhost:8080
2. Find the DAG: `steam_ingestion_pipeline`
3. Enable the dag. Switch the toggle button to right. 
4. Monitor task execution in real-time

### Automatic Hourly Execution

The DAG is configured with `@hourly` schedule. It will automatically run every hour once enabled:

```bash
# Enable the DAG (make it active)
airflow dags unpause steam_ingestion_pipeline
```

To disable:
```bash
airflow dags pause steam_ingestion_pipeline
```

### View Logs

```bash
# Tail logs for a specific task
airflow tasks logs steam_ingestion_pipeline fetch_top_trending_games 2025-01-15T00:00:00Z

# View all logs
tail -f airflow-steam-ingestion/airflow_home/logs/dag_id=steam_ingestion_pipeline/
```

## Database Overview

For detailed database schema documentation, see [README_DATABASE.md](README_DATABASE.md).

### Key Tables

| Table | Type | Rows | Purpose |
|-------|------|------|---------|
| `trending_games` | Bronze | ~100/run | Raw trending game list (hourly snapshot) |
| `game_catalog` | Bronze | Static | Game metadata (one record per game) |
| `player_count` | Bronze | ~100/run | Current player counts (hourly snapshot) |
| `popularity_stats` | Bronze | ~100/run | Game statistics (hourly snapshot) |
| `games_cleaned` | Silver | ~100/run | Analytics-ready KPI snapshots |

## Deduplication Strategy

All tables use UNIQUE constraints on (appid, run_date, run_hour) except `game_catalog`:

```sql
UNIQUE KEY `unique_game_hour` (`appid`, `run_date`, `run_hour`)
```

This ensures:
- Each game appears only once per hour across all runs
- Reruns update existing data without duplication
- Time-series data integrity is maintained
- `max_active_runs=1` prevents concurrent executions

## API Rate Limits & Timeouts

The pipeline includes error handling and retry logic:

- **Connection Timeout**: 10 seconds per API call
- **Retries**: Up to 3 attempts with exponential backoff
- **Backoff Factor**: 0.5 seconds initial delay
- **HTTP Status Retry**: 429, 500-504 status codes
- **Task Retries**: 2 retries with 5-minute delay

If APIs are unavailable, the task will fail and retry. Non-critical data (e.g., game details that already exist) will be skipped.

## Operational Notes

### Data Quality Checks

The pipeline performs automatic validation:
- Drops records with missing `appid` or `name`
- Safely converts numeric strings to integers
- Handles NULL values gracefully
- Validates owner count parsing

### Monitoring

Check database for pipeline health:

```sql
-- Check latest run
SELECT run_date, run_hour, COUNT(*) as game_count
FROM games_cleaned
GROUP BY run_date, run_hour
ORDER BY run_date DESC, run_hour DESC
LIMIT 5;

-- Find missing games (failed API calls)
SELECT tg.appid, tg.name
FROM trending_games tg
WHERE DATE(tg.created_at) = CURDATE()
AND NOT EXISTS (SELECT 1 FROM games_cleaned gc WHERE gc.appid = tg.appid);
```

### Performance Considerations

- **game_catalog** is queried before API calls to skip expensive network requests
- Batch inserts reduce database round-trips
- Indexes on (appid, run_date, run_hour) optimize join operations
- Time-series data remains queryable for historical analysis

## Troubleshooting

### MySQL Connection Error
```
"Failed to connect to MySQL database"
```
**Solution**: Ensure MySQL is running and credentials are correct
```bash
# Check MySQL status
mysql -u root -h localhost -e "SELECT 1;"
```

### Airflow DAG Not Showing
**Solution**: Ensure `dags/` folder exists and DAG file is in Python path
```bash
# Verify DAG syntax
airflow dags validate
```

### Task Failures
**Solution**: Check task logs in Airflow UI or CLI
```bash
airflow tasks logs steam_ingestion_pipeline <task_id> <execution_date>
```

### API Timeouts
**Solution**: These are expected occasionally. The pipeline will retry automatically with exponential backoff.

## Next Steps

1. Monitor the DAG through Airflow UI
2. Verify data in MySQL tables
3. Create downstream analytics queries on `games_cleaned` table
4. Set up alerts for task failures
5. Extend with additional data sources or transformations

## References

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [SteamSpy API](https://steamspy.com/api.php)
- [Steam Web API](https://steamcommunity.com/dev)
- [Steam API Tester](https://steamapi.xpaw.me/)
- [MySQL Documentation](https://dev.mysql.com/doc/)

