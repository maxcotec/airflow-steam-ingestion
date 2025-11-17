"""
Data processing module for cleaning and aggregating game data.
Handles transformation from raw API data to analytics-ready silver table.
"""
import logging
import re
from typing import Dict, List, Any, Optional
from datetime import date

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data cleaning, transformation, and aggregation."""

    @staticmethod
    def parse_owners_string(owners_str: str) -> Optional[int]:
        """
        Extract lower-bound estimate from owners range string.
        
        Example: "10,000,000 .. 20,000,000" → 10000000
        
        Args:
            owners_str: Owners range as string
            
        Returns:
            Lower bound as integer or None
        """
        if not owners_str or not isinstance(owners_str, str):
            return None
        
        try:
            # Extract first number sequence from the string (remove commas first)
            # The owners string is already in absolute format like "10,000,000"
            # We need to reconstruct it properly
            cleaned = owners_str.replace(',', '').split('..')[0].strip()
            if cleaned:
                owner_count = int(cleaned)
                # Cap at INT max to avoid overflow (2^31 - 1 = 2,147,483,647)
                if owner_count > 2147483647:
                    owner_count = 2147483647
                return owner_count
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing owners string '{owners_str}': {e}")
        
        return None

    @staticmethod
    def convert_price_cents_to_usd(price_cents: Any) -> Optional[float]:
        """
        Convert price from cents to USD.
        
        Args:
            price_cents: Price in cents
            
        Returns:
            Price in USD as decimal or None
        """
        if price_cents is None:
            return None
        
        try:
            # Handle string and numeric types
            if isinstance(price_cents, str):
                price_value = float(price_cents)
            else:
                price_value = float(price_cents)
            
            # If value is already in reasonable USD range, return as-is
            if price_value < 1000:
                return price_value
            
            # Otherwise assume it's in cents
            return round(price_value / 100, 2)
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting price '{price_cents}': {e}")
            return None

    @staticmethod
    def safe_int(value: Any, default: int = None) -> Optional[int]:
        """
        Safely convert value to integer.
        
        Args:
            value: Value to convert
            default: Default value if conversion fails
            
        Returns:
            Integer value or default
        """
        if value is None:
            return default
        
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def safe_float(value: Any, default: float = None) -> Optional[float]:
        """
        Safely convert value to float.
        
        Args:
            value: Value to convert
            default: Default value if conversion fails
            
        Returns:
            Float value or default
        """
        if value is None:
            return default
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def clean_game_record(
        trending_game: Dict[str, Any],
        game_catalog: Optional[Dict[str, Any]],
        player_count: Optional[Dict[str, Any]],
        popularity_stats: Optional[Dict[str, Any]],
        run_date: date,
        run_hour: int
    ) -> Optional[Dict[str, Any]]:
        """
        Clean and merge data from all sources into single record.
        
        Args:
            trending_game: Data from trending_games table
            game_catalog: Data from game_catalog table
            player_count: Data from player_count table
            popularity_stats: Data from popularity_stats table
            run_date: Run date from DAG
            run_hour: Run hour from DAG
            
        Returns:
            Cleaned record for games_cleaned table or None if required fields missing
        """
        try:
            # Extract required fields
            appid = trending_game.get("appid")
            name = (game_catalog and game_catalog.get("name")) or trending_game.get("name")
            
            # Drop rows with missing required fields
            if not appid or appid == "" or not name or name == "":
                logger.warning(f"Skipping record: missing required fields (appid={appid}, name={name})")
                return None
            
            # Safe integer conversion
            appid = DataProcessor.safe_int(appid)
            if not appid:
                return None
            
            # Extract time-based metrics
            current_players = None
            if player_count:
                current_players = DataProcessor.safe_int(player_count.get("current_players"))
            
            # Extract popularity stats
            ccu = None
            positive_reviews = None
            negative_reviews = None
            average_playtime_forever = None
            median_playtime_forever = None
            average_playtime_2weeks = None
            median_playtime_2weeks = None
            price_usd = None
            tags = None
            
            if popularity_stats:
                ccu = DataProcessor.safe_int(popularity_stats.get("ccu"))
                positive_reviews = DataProcessor.safe_int(popularity_stats.get("positive"))
                negative_reviews = DataProcessor.safe_int(popularity_stats.get("negative"))
                average_playtime_forever = DataProcessor.safe_int(popularity_stats.get("average_forever"))
                median_playtime_forever = DataProcessor.safe_int(popularity_stats.get("median_forever"))
                average_playtime_2weeks = DataProcessor.safe_int(popularity_stats.get("average_2weeks"))
                median_playtime_2weeks = DataProcessor.safe_int(popularity_stats.get("median_2weeks"))
                price_usd = DataProcessor.convert_price_cents_to_usd(popularity_stats.get("price"))
                tags = popularity_stats.get("tags", "")
            
            # Extract estimated owners
            estimated_owners = None
            if popularity_stats and popularity_stats.get("owners"):
                estimated_owners = DataProcessor.parse_owners_string(popularity_stats.get("owners"))
            
            # Extract game price from catalog if not in popularity_stats
            if not price_usd and game_catalog and game_catalog.get("price"):
                price_usd = DataProcessor.convert_price_cents_to_usd(game_catalog.get("price"))
            
            # Default values for fields that might be missing
            score_rank = 0
            discount_percent = 0
            
            cleaned_record = {
                "run_date": run_date,
                "run_hour": run_hour,
                "appid": appid,
                "name": str(name)[:500],  # Truncate to column size
                "current_players": current_players,
                "ccu": ccu,
                "average_playtime_2weeks": average_playtime_2weeks,
                "median_playtime_2weeks": median_playtime_2weeks,
                "estimated_owners": estimated_owners,
                "positive_reviews": positive_reviews,
                "negative_reviews": negative_reviews,
                "average_playtime_forever": average_playtime_forever,
                "median_playtime_forever": median_playtime_forever,
                "price_usd": price_usd,
                "score_rank": score_rank,
                "discount_percent": discount_percent
            }
            
            return cleaned_record
        except Exception as e:
            logger.error(f"Error cleaning game record: {e}")
            return None

    @staticmethod
    def aggregate_hourly_data(
        trending_games: List[Dict[str, Any]],
        game_catalog_lookup: Dict[int, Dict[str, Any]],
        player_count_lookup: Dict[int, Dict[str, Any]],
        popularity_stats_lookup: Dict[int, Dict[str, Any]],
        run_date: date,
        run_hour: int
    ) -> List[Dict[str, Any]]:
        """
        Aggregate all raw data into hourly KPI snapshots.
        
        Args:
            trending_games: List of trending games for the run
            game_catalog_lookup: Dict lookup of game catalogs by appid
            player_count_lookup: Dict lookup of player counts by appid
            popularity_stats_lookup: Dict lookup of popularity stats by appid
            run_date: Run date from DAG
            run_hour: Run hour from DAG
            
        Returns:
            List of cleaned records ready for insertion
        """
        cleaned_records = []
        
        for trending_game in trending_games:
            appid = trending_game.get("appid")
            
            # Look up data from other sources
            game_catalog = game_catalog_lookup.get(appid)
            player_count = player_count_lookup.get(appid)
            popularity_stats = popularity_stats_lookup.get(appid)
            
            # Clean and aggregate
            cleaned = DataProcessor.clean_game_record(
                trending_game,
                game_catalog,
                player_count,
                popularity_stats,
                run_date,
                run_hour
            )
            
            if cleaned:
                cleaned_records.append(cleaned)
        
        logger.info(f"Aggregated {len(cleaned_records)} cleaned records from {len(trending_games)} trending games")
        return cleaned_records
