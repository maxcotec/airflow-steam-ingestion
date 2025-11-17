"""
Database initialization script.
Run this script to create the steam_games database and all required tables.

Usage:
    python init_db.py
"""
import logging
import sys
from src.database import MySQLConnector, DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_database():
    """Create the steam_games database if it doesn't exist."""
    try:
        # Connect to MySQL root (no database specified)
        conn = MySQLConnector(database='mysql')
        if not conn.connect():
            logger.error("Failed to connect to MySQL")
            return False
        
        # Create database
        create_db_query = """
        CREATE DATABASE IF NOT EXISTS steam_games
        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
        
        if conn.execute_query(create_db_query):
            logger.info("Database 'steam_games' created or already exists")
            conn.disconnect()
            return True
        else:
            logger.error("Failed to create database")
            conn.disconnect()
            return False
    
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        return False


def initialize_tables():
    """Initialize all required tables."""
    try:
        # Connect to steam_games database
        db_connector = MySQLConnector()
        if not db_connector.connect():
            logger.error("Failed to connect to steam_games database")
            return False
        
        # Initialize database manager
        db_manager = DatabaseManager(db_connector)
        
        # Create all tables
        if db_manager.initialize_database():
            logger.info("All tables initialized successfully")
            db_connector.disconnect()
            return True
        else:
            logger.error("Failed to initialize tables")
            db_connector.disconnect()
            return False
    
    except Exception as e:
        logger.error(f"Error initializing tables: {e}")
        return False


def main():
    """Main initialization function."""
    logger.info("Starting database initialization...")
    
    # Step 1: Create database
    logger.info("Step 1: Creating database...")
    if not create_database():
        logger.error("Failed to create database. Exiting.")
        sys.exit(1)
    
    # Step 2: Initialize tables
    logger.info("Step 2: Initializing tables...")
    if not initialize_tables():
        logger.error("Failed to initialize tables. Exiting.")
        sys.exit(1)
    
    logger.info("Database initialization completed successfully!")
    sys.exit(0)


if __name__ == '__main__':
    main()
