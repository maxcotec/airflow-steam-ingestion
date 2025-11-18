"""
Apache Airflow DAG for Steam game data ingestion pipeline.
Runs hourly to fetch trending games, details, player counts, and popularity stats.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from src.database import MySQLConnector, DatabaseManager
from src.steam_api import SteamAPIClient
from src.data_processing import DataProcessor
from src.utils import get_run_date_from_logical_date, get_run_hour_from_logical_date

logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'steam_ingestion_pipeline',
    default_args=default_args,
    description='Steam game data ingestion pipeline',
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,  # Only one run at a time
    tags=['steam-data', 'ingestion'],
)


def fetch_top_trending_games(**context) -> None:
    """
    Task 1: Fetch top 100 trending games from SteamSpy and store in trending_games table.
    
    Args:
        context: Airflow context (contains logical_date)
    """
    logger.info("Starting fetch_top_trending_games task")
    
    logical_date = context['logical_date']
    run_date = get_run_date_from_logical_date(logical_date)
    run_hour = get_run_hour_from_logical_date(logical_date)
    run_date_str = run_date.strftime('%Y-%m-%d')
    logger.info(f"Run Date: {run_date_str}, Run Hour: {run_hour}")
    
    # Initialize connectors
    db_connector = MySQLConnector()
    if not db_connector.connect():
        raise Exception("Failed to connect to MySQL database")
    
    api_client = SteamAPIClient()
    
    try:
        # Fetch trending games
        trending_games = api_client.get_top_100_trending()
        
        if not trending_games:
            logger.warning("No trending games fetched")
            return
        
        logger.info(f"Fetched {len(trending_games)} trending games")
        
        # Prepare data for insertion
        insert_data = [
            (run_date_str, run_hour, game['appid'], game['name'], game.get('median_2weeks'))
            for game in trending_games
        ]
        
        # Insert into trending_games table
        insert_query = """
        INSERT INTO trending_games (run_date, run_hour, appid, name, median_2weeks)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            median_2weeks = VALUES(median_2weeks)
        """
        
        if db_connector.execute_many(insert_query, insert_data):
            logger.info(f"Successfully inserted {len(trending_games)} trending games")
        else:
            raise Exception("Failed to insert trending games")
    
    finally:
        api_client.close()
        db_connector.disconnect()


def fetch_game_details(**context) -> None:
    """
    Task 2: Fetch game details from Steam API for new games and store in game_catalog.
    Skips games already in catalog to minimize API calls.
    
    Args:
        context: Airflow context (contains logical_date)
    """
    logger.info("Starting fetch_game_details task")
    
    logical_date = context['logical_date']
    run_date = get_run_date_from_logical_date(logical_date)
    run_hour = get_run_hour_from_logical_date(logical_date)
    run_date_str = run_date.strftime('%Y-%m-%d')
    logger.info(f"Run Date: {run_date_str}, Run Hour: {run_hour}")
    
    # Initialize connectors
    db_connector = MySQLConnector()
    if not db_connector.connect():
        raise Exception("Failed to connect to MySQL database")
    
    api_client = SteamAPIClient()
    
    try:
        # Fetch appids from trending_games for this run
        query = "SELECT DISTINCT appid FROM trending_games WHERE run_date = %s AND run_hour = %s"
        appids_result = db_connector.fetch_all(query, (run_date_str, run_hour))
        
        appids = [row['appid'] for row in appids_result]
        logger.info(f"Found {len(appids)} appids from trending games")
        
        if not appids:
            logger.warning("No appids found in trending_games")
            return
        
        # Check which appids are already in game_catalog
        placeholders = ','.join(['%s'] * len(appids))
        check_query = f"SELECT appid FROM game_catalog WHERE appid IN ({placeholders})"
        existing_appids = db_connector.fetch_all(check_query, tuple(appids))
        existing_set = {row['appid'] for row in existing_appids}
        
        # Filter to only new appids
        new_appids = [appid for appid in appids if appid not in existing_set]
        logger.info(f"Found {len(new_appids)} new appids to fetch details for")
        
        if not new_appids:
            logger.info("All apps already in catalog, skipping API calls")
            return
        
        # Fetch details for new games
        game_details_list = []
        for appid in new_appids:
            details = api_client.get_game_details(appid)
            if details:
                game_details_list.append(details)
        
        logger.info(f"Fetched details for {len(game_details_list)} games")
        
        if not game_details_list:
            logger.warning("No game details were fetched successfully")
            return
        
        # Insert into game_catalog table
        insert_data = [
            (
                game['appid'],
                game['name'],
                game['developer'],
                game['release_date'],
                game['genres'],
                game['price'],
                game['description'],
                game['platforms']
            )
            for game in game_details_list
        ]
        
        insert_query = """
        INSERT IGNORE INTO game_catalog (appid, name, developer, release_date, genres, price, description, platforms)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        if db_connector.execute_many(insert_query, insert_data):
            logger.info(f"Successfully inserted {len(game_details_list)} game details")
        else:
            raise Exception("Failed to insert game details")
    
    finally:
        api_client.close()
        db_connector.disconnect()


def fetch_player_count(**context) -> None:
    """
    Task 3: Fetch current player count for each trending game.
    
    Args:
        context: Airflow context (contains logical_date)
    """
    logger.info("Starting fetch_player_count task")
    
    logical_date = context['logical_date']
    run_date = get_run_date_from_logical_date(logical_date)
    run_hour = get_run_hour_from_logical_date(logical_date)
    run_date_str = run_date.strftime('%Y-%m-%d')
    logger.info(f"Run Date: {run_date_str}, Run Hour: {run_hour}")
    
    # Initialize connectors
    db_connector = MySQLConnector()
    if not db_connector.connect():
        raise Exception("Failed to connect to MySQL database")
    
    api_client = SteamAPIClient()
    
    try:
        # Fetch appids from trending_games for this run
        query = "SELECT DISTINCT appid FROM trending_games WHERE run_date = %s AND run_hour = %s"
        appids_result = db_connector.fetch_all(query, (run_date_str, run_hour))
        
        appids = [row['appid'] for row in appids_result]
        logger.info(f"Found {len(appids)} appids to fetch player counts for")
        
        if not appids:
            logger.warning("No appids found in trending_games")
            return
        
        # Fetch player counts
        player_counts = []
        for appid in appids:
            count = api_client.get_player_count(appid)
            if count is not None:
                player_counts.append((appid, run_date_str, run_hour, count))
        
        logger.info(f"Fetched player counts for {len(player_counts)} games")
        
        if not player_counts:
            logger.warning("No player counts were fetched successfully")
            return
        
        # Insert into player_count table
        insert_query = """
        INSERT INTO player_count (appid, run_date, run_hour, current_players)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            current_players = VALUES(current_players),
            timestamp = CURRENT_TIMESTAMP
        """
        
        if db_connector.execute_many(insert_query, player_counts):
            logger.info(f"Successfully inserted {len(player_counts)} player counts")
        else:
            raise Exception("Failed to insert player counts")
    
    finally:
        api_client.close()
        db_connector.disconnect()


def fetch_popularity_stats(**context) -> None:
    """
    Task 4: Fetch popularity statistics from SteamSpy for trending games.
    
    Args:
        context: Airflow context (contains logical_date)
    """
    logger.info("Starting fetch_popularity_stats task")
    
    logical_date = context['logical_date']
    run_date = get_run_date_from_logical_date(logical_date)
    run_hour = get_run_hour_from_logical_date(logical_date)
    run_date_str = run_date.strftime('%Y-%m-%d')
    logger.info(f"Run Date: {run_date_str}, Run Hour: {run_hour}")
    
    # Initialize connectors
    db_connector = MySQLConnector()
    if not db_connector.connect():
        raise Exception("Failed to connect to MySQL database")
    
    api_client = SteamAPIClient()
    
    try:
        # Fetch appids from trending_games for this run
        query = "SELECT DISTINCT appid FROM trending_games WHERE run_date = %s AND run_hour = %s"
        appids_result = db_connector.fetch_all(query, (run_date_str, run_hour))
        
        appids = [row['appid'] for row in appids_result]
        logger.info(f"Found {len(appids)} appids to fetch popularity stats for")
        
        if not appids:
            logger.warning("No appids found in trending_games")
            return
        
        # Fetch popularity stats
        stats_list = []
        for appid in appids:
            stats = api_client.get_popularity_stats(appid)
            if stats:
                stats_list.append(stats)
        
        logger.info(f"Fetched popularity stats for {len(stats_list)} games")
        
        if not stats_list:
            logger.warning("No popularity stats were fetched successfully")
            return
        
        # Prepare data for insertion
        insert_data = [
            (
                stat['appid'],
                run_date_str,
                run_hour,
                stat['owners'],
                stat['ccu'],
                stat['positive'],
                stat['negative'],
                stat['average_forever'],
                stat['average_2weeks'],
                stat['median_forever'],
                stat['median_2weeks'],
                stat['price'],
                stat['tags']
            )
            for stat in stats_list
        ]
        
        # Insert into popularity_stats table
        insert_query = """
        INSERT INTO popularity_stats 
        (appid, run_date, run_hour, owners, ccu, positive, negative, 
         average_forever, average_2weeks, median_forever, median_2weeks, price, tags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            owners = VALUES(owners),
            ccu = VALUES(ccu),
            positive = VALUES(positive),
            negative = VALUES(negative),
            average_forever = VALUES(average_forever),
            average_2weeks = VALUES(average_2weeks),
            median_forever = VALUES(median_forever),
            median_2weeks = VALUES(median_2weeks),
            price = VALUES(price),
            tags = VALUES(tags),
            timestamp = CURRENT_TIMESTAMP
        """
        
        if db_connector.execute_many(insert_query, insert_data):
            logger.info(f"Successfully inserted {len(insert_data)} popularity stats")
        else:
            raise Exception("Failed to insert popularity stats")
    
    finally:
        api_client.close()
        db_connector.disconnect()


def merge_and_clean(**context) -> None:
    """
    Task 5: Clean, transform, and aggregate all data into games_cleaned table.
    Combines data from trending_games, game_catalog, player_count, and popularity_stats.
    
    Args:
        context: Airflow context (contains logical_date)
    """
    logger.info("Starting merge_and_clean task")
    
    logical_date = context['logical_date']
    run_date = get_run_date_from_logical_date(logical_date)
    run_hour = get_run_hour_from_logical_date(logical_date)
    run_date_str = run_date.strftime('%Y-%m-%d')
    logger.info(f"Run Date: {run_date_str}, Run Hour: {run_hour}")
    
    # Initialize database connector
    db_connector = MySQLConnector()
    if not db_connector.connect():
        raise Exception("Failed to connect to MySQL database")
    
    try:
        # Fetch trending games for this run
        trending_query = """
        SELECT appid, name, median_2weeks
        FROM trending_games
        WHERE run_date = %s AND run_hour = %s
        ORDER BY appid
        """
        trending_games = db_connector.fetch_all(trending_query, (run_date_str, run_hour))
        logger.info(f"Fetched {len(trending_games)} trending games")
        
        if not trending_games:
            logger.warning("No trending games found for this run")
            return
        
        # Fetch all appids for lookups
        appids = [game['appid'] for game in trending_games]
        
        # Fetch game catalog data
        placeholders = ','.join(['%s'] * len(appids))
        catalog_query = f"""
        SELECT appid, name, developer, release_date, genres, price, description, platforms
        FROM game_catalog
        WHERE appid IN ({placeholders})
        """
        catalog_results = db_connector.fetch_all(catalog_query, tuple(appids))
        game_catalog_lookup = {row['appid']: row for row in catalog_results}
        logger.info(f"Fetched {len(game_catalog_lookup)} games from catalog")
        
        # Fetch player count data
        player_query = f"""
        SELECT appid, current_players
        FROM player_count
        WHERE run_date = %s AND run_hour = %s AND appid IN ({placeholders})
        """
        player_results = db_connector.fetch_all(player_query, (run_date_str, run_hour) + tuple(appids))
        player_count_lookup = {row['appid']: row for row in player_results}
        logger.info(f"Fetched player counts for {len(player_count_lookup)} games")
        
        # Fetch popularity stats data
        stats_query = f"""
        SELECT appid, owners, ccu, positive, negative, average_forever, average_2weeks,
               median_forever, median_2weeks, price, tags
        FROM popularity_stats
        WHERE run_date = %s AND run_hour = %s AND appid IN ({placeholders})
        """
        stats_results = db_connector.fetch_all(stats_query, (run_date_str, run_hour) + tuple(appids))
        popularity_stats_lookup = {row['appid']: row for row in stats_results}
        logger.info(f"Fetched stats for {len(popularity_stats_lookup)} games")
        
        # Aggregate and clean data
        cleaned_records = DataProcessor.aggregate_hourly_data(
            trending_games,
            game_catalog_lookup,
            player_count_lookup,
            popularity_stats_lookup,
            run_date,
            run_hour
        )
        
        logger.info(f"Generated {len(cleaned_records)} cleaned records")
        
        if not cleaned_records:
            logger.warning("No cleaned records generated")
            return
        
        # Prepare data for insertion
        insert_data = [
            (
                record['run_date'].strftime('%Y-%m-%d'),
                record['run_hour'],
                record['appid'],
                record['name'],
                record['current_players'],
                record['ccu'],
                record['average_playtime_2weeks'],
                record['median_playtime_2weeks'],
                record['estimated_owners'],
                record['positive_reviews'],
                record['negative_reviews'],
                record['average_playtime_forever'],
                record['median_playtime_forever'],
                record['price_usd'],
                record['score_rank'],
                record['discount_percent']
            )
            for record in cleaned_records
        ]
        
        # Insert into games_cleaned table
        insert_query = """
        INSERT INTO games_cleaned 
        (run_date, run_hour, appid, name, current_players, ccu, average_playtime_2weeks,
         median_playtime_2weeks, estimated_owners, positive_reviews, negative_reviews,
         average_playtime_forever, median_playtime_forever, price_usd, score_rank, discount_percent)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            current_players = VALUES(current_players),
            ccu = VALUES(ccu),
            average_playtime_2weeks = VALUES(average_playtime_2weeks),
            median_playtime_2weeks = VALUES(median_playtime_2weeks),
            estimated_owners = VALUES(estimated_owners),
            positive_reviews = VALUES(positive_reviews),
            negative_reviews = VALUES(negative_reviews),
            average_playtime_forever = VALUES(average_playtime_forever),
            median_playtime_forever = VALUES(median_playtime_forever),
            price_usd = VALUES(price_usd),
            score_rank = VALUES(score_rank),
            discount_percent = VALUES(discount_percent),
            created_at = CURRENT_TIMESTAMP
        """
        
        if db_connector.execute_many(insert_query, insert_data):
            logger.info(f"Successfully inserted {len(cleaned_records)} cleaned records into games_cleaned")
        else:
            raise Exception("Failed to insert cleaned records")
    
    finally:
        db_connector.disconnect()


# Define tasks
task_fetch_trending = PythonOperator(
    task_id='fetch_top_trending_games',
    python_callable=fetch_top_trending_games,
    dag=dag,
)

task_fetch_details = PythonOperator(
    task_id='fetch_game_details',
    python_callable=fetch_game_details,
    dag=dag,
)

task_fetch_players = PythonOperator(
    task_id='fetch_player_count',
    python_callable=fetch_player_count,
    dag=dag,
)

task_fetch_stats = PythonOperator(
    task_id='fetch_popularity_stats',
    python_callable=fetch_popularity_stats,
    dag=dag,
)

task_merge_clean = PythonOperator(
    task_id='merge_and_clean',
    python_callable=merge_and_clean,
    dag=dag,
)

# Define dependencies
task_fetch_trending >> [task_fetch_details, task_fetch_players, task_fetch_stats] >> task_merge_clean
