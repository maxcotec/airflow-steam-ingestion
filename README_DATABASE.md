# Database Schema Documentation

Complete database schema for the Steam game data ingestion pipeline. All tables are stored in the `steam_games` MySQL database.

## Database Connection

```
Host: localhost
Port: 3306
User: root
Password: (none/empty)
Database: steam_games
Character Set: utf8mb4
Collation: utf8mb4_unicode_ci
```

## Data Architecture

The pipeline follows a **Bronze → Silver** layered architecture:

- **Bronze Layer** (raw, time-series data):
  - `trending_games` - Raw trending game snapshots
  - `game_catalog` - Static game metadata
  - `player_count` - Raw player count snapshots
  - `popularity_stats` - Raw popularity statistics

- **Silver Layer** (cleaned, aggregated):
  - `games_cleaned` - Analytics-ready KPI snapshots

---

## Table Schemas

### 1. trending_games (Bronze Layer)

**Purpose**: Stores the top 100 trending games from SteamSpy API for each hourly run.

```sql
CREATE TABLE `trending_games` (
    `id` int NOT NULL AUTO_INCREMENT,
    `run_date` date NOT NULL,
    `run_hour` int NOT NULL,
    `appid` int NOT NULL,
    `name` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `median_2weeks` int DEFAULT NULL,
    `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_trending_hour` (`appid`,`run_date`,`run_hour`),
    KEY `idx_run_date` (`run_date`),
    KEY `idx_appid` (`appid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | int | NO | Auto-increment primary key |
| `run_date` | date | NO | Date of pipeline run (YYYY-MM-DD) |
| `run_hour` | int | NO | Hour of pipeline run (0-23) |
| `appid` | int | NO | Steam application ID |
| `name` | varchar(500) | YES | Game name |
| `median_2weeks` | int | YES | Median playtime in last 2 weeks |
| `created_at` | timestamp | YES | Record creation timestamp |

**Indexes**:
- `unique_trending_hour`: Ensures no duplicates per hour per game
- `idx_run_date`: Optimizes filtering by run date
- `idx_appid`: Optimizes game lookups

**Data Characteristics**:
- ~100 rows inserted per DAG run
- Updated hourly with `@hourly` schedule
- Upserted: If duplicate found, name and median_2weeks updated
- Retention: Indefinite (time-series data)

**Query Examples**:
```sql
-- Find all games trending today
SELECT * FROM trending_games WHERE run_date = CURDATE();

-- Track a specific game's trending history
SELECT * FROM trending_games WHERE appid = 570 ORDER BY run_date, run_hour;

-- Games trending in specific hour
SELECT * FROM trending_games WHERE run_date = '2025-01-15' AND run_hour = 14;
```

---

### 2. game_catalog (Bronze Layer)

**Purpose**: Static reference table for game metadata. One record per game, populated from Steam Store API.

```sql
CREATE TABLE `game_catalog` (
    `appid` int NOT NULL,
    `name` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `developer` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `release_date` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `genres` text COLLATE utf8mb4_unicode_ci,
    `price` decimal(10,2) DEFAULT NULL,
    `description` text COLLATE utf8mb4_unicode_ci,
    `platforms` text COLLATE utf8mb4_unicode_ci,
    `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`appid`),
    KEY `idx_name` (`name`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `appid` | int | NO | Steam application ID (primary key) |
| `name` | varchar(500) | YES | Game name |
| `developer` | varchar(500) | YES | Primary developer |
| `release_date` | varchar(100) | YES | Release date string |
| `genres` | text | YES | Comma-separated genres |
| `price` | decimal(10,2) | YES | Price in USD |
| `description` | text | YES | Short game description |
| `platforms` | text | YES | Comma-separated platforms (windows, mac, linux) |
| `created_at` | timestamp | YES | Record insertion timestamp |

**Indexes**:
- `PRIMARY KEY`: Direct appid lookups
- `idx_name`: Search games by name

**Data Characteristics**:
- Static table (no run_date/run_hour)
- One record per unique game
- Inserted only for new games (via INSERT IGNORE in DAG)
- Updated manually if game metadata changes
- Retention: Indefinite

**Optimization**:
- DAG checks this table before calling Steam API
- If game already exists, API call is skipped
- Significantly reduces API load over time

**Query Examples**:
```sql
-- Get game details
SELECT * FROM game_catalog WHERE appid = 570;

-- Find all games by developer
SELECT * FROM game_catalog WHERE developer LIKE '%Valve%';

-- Find free games
SELECT * FROM game_catalog WHERE price = 0 OR price IS NULL;

-- Find Linux games
SELECT * FROM game_catalog WHERE platforms LIKE '%linux%';
```

---

### 3. player_count (Bronze Layer)

**Purpose**: Records current player count for each game at each hourly run.

```sql
CREATE TABLE `player_count` (
    `id` int NOT NULL AUTO_INCREMENT,
    `appid` int NOT NULL,
    `run_date` date NOT NULL,
    `run_hour` int NOT NULL,
    `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    `current_players` int DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_count_hour` (`appid`,`run_date`,`run_hour`),
    KEY `idx_appid_rundate` (`appid`,`run_date`,`run_hour`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | int | NO | Auto-increment primary key |
| `appid` | int | NO | Steam application ID |
| `run_date` | date | NO | Date of pipeline run |
| `run_hour` | int | NO | Hour of pipeline run (0-23) |
| `timestamp` | timestamp | YES | Record creation time |
| `current_players` | int | YES | Current player count |

**Indexes**:
- `unique_count_hour`: Ensures one record per hour per game
- `idx_appid_rundate`: Optimizes time-series queries

**Data Characteristics**:
- ~100 rows per DAG run
- Updated hourly with current player counts
- Upserted: Updates values on duplicate
- Retention: Indefinite (time-series data)

**Query Examples**:
```sql
-- Current players for a game
SELECT * FROM player_count WHERE appid = 570 ORDER BY run_date DESC, run_hour DESC LIMIT 1;

-- Player count trend over 24 hours
SELECT run_date, run_hour, current_players FROM player_count 
WHERE appid = 570 AND run_date = CURDATE() 
ORDER BY run_hour;

-- Peak player count today
SELECT appid, MAX(current_players) as peak_players 
FROM player_count WHERE run_date = CURDATE() 
GROUP BY appid ORDER BY peak_players DESC LIMIT 10;
```

---

### 4. popularity_stats (Bronze Layer)

**Purpose**: Stores historical popularity metrics from SteamSpy API for each hourly run.

```sql
CREATE TABLE `popularity_stats` (
    `id` int NOT NULL AUTO_INCREMENT,
    `appid` int NOT NULL,
    `run_date` date NOT NULL,
    `run_hour` int NOT NULL,
    `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    `owners` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `ccu` int DEFAULT NULL,
    `positive` int DEFAULT NULL,
    `negative` int DEFAULT NULL,
    `average_forever` int DEFAULT NULL,
    `average_2weeks` int DEFAULT NULL,
    `median_forever` int DEFAULT NULL,
    `median_2weeks` int DEFAULT NULL,
    `price` decimal(10,2) DEFAULT NULL,
    `tags` text COLLATE utf8mb4_unicode_ci,
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_popularity_hour` (`appid`,`run_date`,`run_hour`),
    KEY `idx_appid_rundate` (`appid`,`run_date`,`run_hour`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | int | NO | Auto-increment primary key |
| `appid` | int | NO | Steam application ID |
| `run_date` | date | NO | Date of pipeline run |
| `run_hour` | int | NO | Hour of pipeline run (0-23) |
| `timestamp` | timestamp | YES | Record creation time |
| `owners` | varchar(100) | YES | Owner count range (e.g., "1,000,000 .. 2,000,000") |
| `ccu` | int | YES | Concurrent users |
| `positive` | int | YES | Number of positive reviews |
| `negative` | int | YES | Number of negative reviews |
| `average_forever` | int | YES | Average playtime all-time (minutes) |
| `average_2weeks` | int | YES | Average playtime last 2 weeks (minutes) |
| `median_forever` | int | YES | Median playtime all-time (minutes) |
| `median_2weeks` | int | YES | Median playtime last 2 weeks (minutes) |
| `price` | decimal(10,2) | YES | Price in cents (divide by 100 for USD) |
| `tags` | text | YES | Comma-separated game tags |

**Indexes**:
- `unique_popularity_hour`: Ensures one record per hour per game
- `idx_appid_rundate`: Optimizes time-series queries

**Data Characteristics**:
- ~100 rows per DAG run
- Updated hourly with popularity stats
- Upserted: Updates values on duplicate
- Retention: Indefinite (time-series data)

**Note on `owners` field**:
- Returns as range string: "1,000,000 .. 2,000,000"
- Data cleaning task extracts lower bound (1,000,000 in example)
- Multiplied by 1,000,000 to get absolute number

**Query Examples**:
```sql
-- Get stats for a game
SELECT * FROM popularity_stats WHERE appid = 570 ORDER BY run_date DESC LIMIT 1;

-- Review sentiment ratio
SELECT appid, 
    positive, 
    negative, 
    ROUND(positive / (positive + negative) * 100, 2) as positive_ratio
FROM popularity_stats WHERE run_date = CURDATE() 
ORDER BY positive_ratio DESC;

-- Playtime trends
SELECT run_date, run_hour, average_2weeks, median_2weeks 
FROM popularity_stats WHERE appid = 570 
ORDER BY run_date DESC, run_hour DESC LIMIT 28;
```

---

### 5. games_cleaned (Silver Layer)

**Purpose**: Analytics-ready table with cleaned, deduplicated, and aggregated hourly KPI snapshots per game.

```sql
CREATE TABLE `games_cleaned` (
    `id` int NOT NULL AUTO_INCREMENT,
    `run_date` date NOT NULL,
    `run_hour` int NOT NULL,
    `appid` int NOT NULL,
    `name` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL,
    `current_players` int DEFAULT NULL,
    `ccu` int DEFAULT NULL,
    `average_playtime_2weeks` int DEFAULT NULL,
    `median_playtime_2weeks` int DEFAULT NULL,
    `estimated_owners` int DEFAULT NULL,
    `positive_reviews` int DEFAULT NULL,
    `negative_reviews` int DEFAULT NULL,
    `average_playtime_forever` int DEFAULT NULL,
    `median_playtime_forever` int DEFAULT NULL,
    `price_usd` decimal(10,2) DEFAULT NULL,
    `score_rank` int DEFAULT '0',
    `discount_percent` int DEFAULT '0',
    `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_game_hour` (`appid`,`run_date`,`run_hour`),
    KEY `idx_appid_rundate` (`appid`,`run_date`,`run_hour`),
    KEY `idx_run_date` (`run_date`),
    KEY `idx_name` (`name`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
```

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | int | NO | Auto-increment primary key |
| `run_date` | date | NO | Date of pipeline run |
| `run_hour` | int | NO | Hour of pipeline run (0-23) |
| `appid` | int | NO | Steam application ID |
| `name` | varchar(500) | NO | Game name (required) |
| `current_players` | int | YES | Current player count (from player_count) |
| `ccu` | int | YES | Concurrent users (from popularity_stats) |
| `average_playtime_2weeks` | int | YES | Avg playtime last 2 weeks in minutes |
| `median_playtime_2weeks` | int | YES | Median playtime last 2 weeks in minutes |
| `estimated_owners` | int | YES | Lower-bound owner estimate in millions |
| `positive_reviews` | int | YES | Number of positive reviews |
| `negative_reviews` | int | YES | Number of negative reviews |
| `average_playtime_forever` | int | YES | Avg playtime all-time in minutes |
| `median_playtime_forever` | int | YES | Median playtime all-time in minutes |
| `price_usd` | decimal(10,2) | YES | Price in USD (cleaned from cents) |
| `score_rank` | int | YES | Game score ranking (default: 0) |
| `discount_percent` | int | YES | Current discount percentage (default: 0) |
| `created_at` | timestamp | YES | Record creation timestamp |

**Indexes**:
- `PRIMARY KEY`: Direct record lookups
- `unique_game_hour`: Ensures one record per hour per game
- `idx_appid_rundate`: Fast time-series queries
- `idx_run_date`: Queries by date
- `idx_name`: Full-text search support

**Data Characteristics**:
- ~100 rows per DAG run (after deduplication)
- Merged from all bronze tables
- Cleaned and transformed data
- Ready for analytics and BI tools
- Retention: Indefinite (primary analytics table)

**Transformations Applied**:
1. **Data Cleaning**:
   - Removed rows with missing appid or name
   - Converted numeric strings to integers
   - Standardized NULL values
   - Truncated long strings to column size

2. **Price Conversion**:
   - Converted from cents to USD: `price_cents / 100`
   - Handled decimal precision

3. **Owner Estimation**:
   - Parsed "10,000,000 .. 20,000,000" format
   - Extracted lower bound: 10,000,000
   - Stored as absolute number (not millions)

4. **Aggregation**:
   - Created hourly snapshots per game
   - One record per (appid, run_date, run_hour)
   - Maintains time-series granularity

**Query Examples**:
```sql
-- Top games by current players (latest hour)
SELECT run_date, run_hour, name, current_players, ccu, price_usd
FROM games_cleaned 
WHERE run_date = CURDATE() AND run_hour = (SELECT MAX(run_hour) FROM games_cleaned WHERE run_date = CURDATE())
ORDER BY current_players DESC LIMIT 10;

-- Game's hourly trend for last 24 hours
SELECT run_date, run_hour, current_players, ccu, average_playtime_2weeks
FROM games_cleaned 
WHERE appid = 570 AND run_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
ORDER BY run_date, run_hour;

-- Calculate review sentiment
SELECT appid, name,
    positive_reviews,
    negative_reviews,
    ROUND(positive_reviews / (positive_reviews + negative_reviews) * 100, 2) as sentiment_ratio
FROM games_cleaned 
WHERE run_date = CURDATE()
ORDER BY sentiment_ratio DESC;

-- Owner analysis
SELECT name, estimated_owners, price_usd, current_players
FROM games_cleaned 
WHERE run_date = CURDATE() AND run_hour = 12
AND estimated_owners > 10000000
ORDER BY estimated_owners DESC;
```

---

## Database Statistics

| Table | Type | Typical Rows | Growing | Static |
|-------|------|--------------|---------|--------|
| trending_games | Bronze | 100/hour | ✓ | |
| game_catalog | Bronze | 100s-1000s | ✓ | ✓ |
| player_count | Bronze | 100/hour | ✓ | |
| popularity_stats | Bronze | 100/hour | ✓ | |
| games_cleaned | Silver | 100/hour | ✓ | |

**Storage Estimate** (per year):
- ~876,000 rows per table (365 days × 24 hours × 100 games)
- Estimated size: ~100-200 MB per bronze table
- games_cleaned: ~150 MB

---

## Deduplication & Idempotency

All tables include UNIQUE constraints to prevent duplicates:

```sql
-- Trending Games
UNIQUE KEY `unique_trending_hour` (`appid`, `run_date`, `run_hour`)

-- Player Count
UNIQUE KEY `unique_count_hour` (`appid`, `run_date`, `run_hour`)

-- Popularity Stats
UNIQUE KEY `unique_popularity_hour` (`appid`, `run_date`, `run_hour`)

-- Games Cleaned
UNIQUE KEY `unique_game_hour` (`appid`, `run_date`, `run_hour`)
```

All inserts use `ON DUPLICATE KEY UPDATE` to be idempotent. If a DAG run is rerun/retried, data is updated instead of duplicated.

---

## Data Quality & Validation

### Automatic Checks (in merge_and_clean task)
- ✓ Required fields: appid, name (NOT NULL)
- ✓ Safe type conversions (numeric strings to INT)
- ✓ NULL handling (graceful defaults)
- ✓ String truncation (to column size)
- ✓ Owner string parsing validation
- ✓ Price conversion safety

### Manual Validation Queries
```sql
-- Check for missing required fields
SELECT COUNT(*) FROM games_cleaned 
WHERE appid IS NULL OR name IS NULL OR name = '';

-- Verify unique constraints
SELECT appid, run_date, run_hour, COUNT(*) as count
FROM games_cleaned
GROUP BY appid, run_date, run_hour
HAVING count > 1;

-- Check data completeness
SELECT 
    COUNT(DISTINCT run_date, run_hour) as runs,
    COUNT(*) as total_records,
    COUNT(*) / COUNT(DISTINCT run_date, run_hour) as avg_games_per_run
FROM games_cleaned;
```

---

## Performance Tuning

### Indexing Strategy
- Composite indexes on (appid, run_date, run_hour) for time-series queries
- Individual indexes on frequently filtered columns
- Primary key indexes for direct lookups

### Query Optimization
```sql
-- ✓ GOOD: Uses composite index
SELECT * FROM games_cleaned 
WHERE appid = 570 AND run_date = '2025-01-15' AND run_hour = 12;

-- ✓ GOOD: Uses date index
SELECT * FROM games_cleaned WHERE run_date = '2025-01-15';

-- ⚠ SLOWER: Full table scan
SELECT * FROM games_cleaned WHERE current_players > 10000;
```

### Storage Optimization
- UTF8MB4 charset for international characters
- Decimal types for prices (avoids floating point issues)
- INT for counts (smaller than BIGINT)
- VARCHAR(500) sufficient for game names
- TEXT fields for descriptions and tags

---

## Maintenance Tasks

### Daily Health Check
```sql
-- Verify latest run completed
SELECT MAX(run_date) as latest_date, MAX(run_hour) as latest_hour
FROM games_cleaned;

-- Count records by date
SELECT run_date, COUNT(*) as record_count
FROM games_cleaned
WHERE run_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY run_date;

-- Check for NULL values
SELECT 
    COUNT(IF(current_players IS NULL, 1, NULL)) as null_players,
    COUNT(IF(ccu IS NULL, 1, NULL)) as null_ccu,
    COUNT(IF(price_usd IS NULL, 1, NULL)) as null_price
FROM games_cleaned WHERE run_date = CURDATE();
```

### Backup Strategy
```bash
# Backup steam_games database
mysqldump -u root steam_games > backup_$(date +%Y%m%d_%H%M%S).sql

# Restore from backup
mysql -u root steam_games < backup_20250115_120000.sql
```

---

## References

- [Steam Web API Documentation](https://steamcommunity.com/dev)
- [SteamSpy API Reference](https://steamspy.com/api)
- [MySQL Data Types](https://dev.mysql.com/doc/refman/8.0/en/data-types.html)
- [Query Optimization](https://dev.mysql.com/doc/refman/8.0/en/optimization.html)
