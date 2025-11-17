"""
Database utilities for MySQL connection and table management.
Handles all database operations including table creation, inserts, and queries.
"""
import logging
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional
from datetime import date

logger = logging.getLogger(__name__)


class MySQLConnector:
    """MySQL connection handler and query executor."""

    def __init__(self, host: str = "localhost", user: str = "root", password: str = None, database: str = "steam_games"):
        """
        Initialize MySQL connector.
        
        Args:
            host: MySQL host
            user: MySQL user
            password: MySQL password
            database: Database name
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self) -> bool:
        """
        Establish connection to MySQL database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                logger.info(f"Connected to MySQL database: {self.database}")
                return True
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return False
        return False

    def disconnect(self):
        """Close MySQL connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")

    def execute_query(self, query: str, params: tuple = None) -> bool:
        """
        Execute a query (INSERT, UPDATE, DELETE).
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            logger.debug(f"Query executed successfully. Rows affected: {cursor.rowcount}")
            cursor.close()
            return True
        except Error as e:
            logger.error(f"Error executing query: {e}")
            self.connection.rollback()
            return False

    def execute_many(self, query: str, data: List[tuple]) -> bool:
        """
        Execute multiple rows insert/update.
        
        Args:
            query: SQL query string with placeholders
            data: List of tuples with values
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, data)
            self.connection.commit()
            logger.info(f"Batch insert/update successful. Rows affected: {cursor.rowcount}")
            cursor.close()
            return True
        except Error as e:
            logger.error(f"Error executing batch query: {e}")
            self.connection.rollback()
            return False

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Fetch all results from a SELECT query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries with results
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Error as e:
            logger.error(f"Error fetching results: {e}")
            return []

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """
        Fetch first result from a SELECT query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Dictionary with first result or None
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            return result
        except Error as e:
            logger.error(f"Error fetching result: {e}")
            return None

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.
        
        Args:
            table_name: Name of the table
            
        Returns:
            bool: True if table exists, False otherwise
        """
        query = f"SELECT COUNT(*) as count FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        result = self.fetch_one(query, (self.database, table_name))
        return result['count'] > 0 if result else False


class DatabaseManager:
    """Manages database initialization and table creation."""

    def __init__(self, connector: MySQLConnector):
        """
        Initialize database manager.
        
        Args:
            connector: MySQLConnector instance
        """
        self.connector = connector

    def initialize_database(self) -> bool:
        """
        Initialize all required tables if they don't exist.
        
        Returns:
            bool: True if successful
        """
        tables_created = []
        
        if not self.connector.table_exists("trending_games"):
            if self._create_trending_games_table():
                tables_created.append("trending_games")
        
        if not self.connector.table_exists("game_catalog"):
            if self._create_game_catalog_table():
                tables_created.append("game_catalog")
        
        if not self.connector.table_exists("player_count"):
            if self._create_player_count_table():
                tables_created.append("player_count")
        
        if not self.connector.table_exists("popularity_stats"):
            if self._create_popularity_stats_table():
                tables_created.append("popularity_stats")
        
        if not self.connector.table_exists("games_cleaned"):
            if self._create_games_cleaned_table():
                tables_created.append("games_cleaned")
        
        if tables_created:
            logger.info(f"Created tables: {', '.join(tables_created)}")
        else:
            logger.info("All tables already exist")
        
        return True

    def _create_trending_games_table(self) -> bool:
        """Create trending_games table."""
        query = """
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
        """
        return self.connector.execute_query(query)

    def _create_game_catalog_table(self) -> bool:
        """Create game_catalog table."""
        query = """
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
        """
        return self.connector.execute_query(query)

    def _create_player_count_table(self) -> bool:
        """Create player_count table."""
        query = """
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
        """
        return self.connector.execute_query(query)

    def _create_popularity_stats_table(self) -> bool:
        """Create popularity_stats table."""
        query = """
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
        """
        return self.connector.execute_query(query)

    def _create_games_cleaned_table(self) -> bool:
        """Create games_cleaned table."""
        query = """
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
        """
        return self.connector.execute_query(query)
