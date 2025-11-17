"""
Steam and SteamSpy API client for fetching game data.
Handles API requests with error handling and retry logic.
"""
import logging
import requests
import time
from typing import Dict, List, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# API endpoints
STEAMSPY_TOP100_URL = "https://steamspy.com/api.php?request=top100in2weeks"
STEAMSPY_APPDETAILS_URL = "https://steamspy.com/api.php?request=appdetails&appid={appid}"
STEAM_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={appid}"
STEAM_PLAYER_COUNT_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid}"


class SteamAPIClient:
    """Client for Steam and SteamSpy API calls."""

    def __init__(self, timeout: int = 10, max_retries: int = 3, backoff_factor: float = 0.5):
        """
        Initialize Steam API client.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            backoff_factor: Backoff factor for exponential backoff
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry strategy.
        
        Returns:
            requests.Session with retry configuration
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_top_100_trending(self) -> List[Dict[str, Any]]:
        """
        Fetch top 100 trending games from SteamSpy.
        
        Returns:
            List of game dictionaries with appid and name
        """
        try:
            logger.info("Fetching top 100 trending games from SteamSpy")
            response = self.session.get(STEAMSPY_TOP100_URL, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            games = []
            
            for appid, game_info in data.items():
                if appid.isdigit():  # Skip non-numeric keys
                    game = {
                        "appid": int(appid),
                        "name": game_info.get("name", ""),
                        "median_2weeks": game_info.get("median_2weeks", 0)
                    }
                    games.append(game)
            
            logger.info(f"Successfully fetched {len(games)} trending games")
            return games
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching top 100 trending games: {e}")
            return []

    def get_game_details(self, appid: int) -> Optional[Dict[str, Any]]:
        """
        Fetch game details from Steam Store API.
        
        Args:
            appid: Steam game application ID
            
        Returns:
            Dictionary with game details or None if error
        """
        try:
            url = STEAM_APPDETAILS_URL.format(appid=appid)
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if str(appid) not in data:
                logger.warning(f"App ID {appid} not found in Steam API response")
                return None
            
            app_data = data[str(appid)]
            
            if not app_data.get("success"):
                logger.warning(f"App ID {appid} returned success=false from Steam API")
                return None
            
            app_info = app_data.get("data", {})
            
            # Extract platforms information
            platforms = app_info.get("platforms", {})
            platforms_str = ",".join([k for k, v in platforms.items() if v])
            
            # Extract genres
            genres = app_info.get("genres", [])
            genres_str = ",".join([g.get("description", "") for g in genres])
            
            game_detail = {
                "appid": appid,
                "name": app_info.get("name", ""),
                "developer": app_info.get("developers", [""])[0] if app_info.get("developers") else "",
                "release_date": app_info.get("release_date", {}).get("date", ""),
                "genres": genres_str,
                "price": app_info.get("price_overview", {}).get("final_price"),
                "description": app_info.get("short_description", ""),
                "platforms": platforms_str
            }
            
            logger.debug(f"Successfully fetched details for app ID {appid}")
            return game_detail
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error fetching game details for app ID {appid}: {e}")
            return None
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing game details for app ID {appid}: {e}")
            return None

    def get_player_count(self, appid: int) -> Optional[int]:
        """
        Fetch current player count for a game.
        
        Args:
            appid: Steam game application ID
            
        Returns:
            Current player count or None if error
        """
        try:
            url = STEAM_PLAYER_COUNT_URL.format(appid=appid)
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # API response structure: {"response": {"player_count": 805055, "result": 1}}
            response_data = data.get("response", {})
            result_code = response_data.get("result")
            
            if result_code == 1:
                player_count = response_data.get("player_count", 0)
                logger.debug(f"Player count for app ID {appid}: {player_count}")
                return player_count
            else:
                logger.warning(f"Error fetching player count for app ID {appid}: result={result_code}")
                return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error fetching player count for app ID {appid}: {e}")
            return None
        except (ValueError, KeyError) as e:
            logger.warning(f"Parse error fetching player count for app ID {appid}: {e}")
            return None

    def get_popularity_stats(self, appid: int) -> Optional[Dict[str, Any]]:
        """
        Fetch popularity stats from SteamSpy.
        
        Args:
            appid: Steam game application ID
            
        Returns:
            Dictionary with popularity stats or None if error
        """
        try:
            url = STEAMSPY_APPDETAILS_URL.format(appid=appid)
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle tags - could be dict with 'tag' key or string
            tags_list = []
            if data.get("tags"):
                for tag in data.get("tags", []):
                    if isinstance(tag, dict):
                        tags_list.append(tag.get("tag", ""))
                    elif isinstance(tag, str):
                        tags_list.append(tag)
            tags_str = ",".join(tags_list)
            
            stats = {
                "appid": appid,
                "owners": data.get("owners", ""),
                "ccu": data.get("ccu", 0),
                "positive": data.get("positive", 0),
                "negative": data.get("negative", 0),
                "average_forever": data.get("average_forever", 0),
                "average_2weeks": data.get("average_2weeks", 0),
                "median_forever": data.get("median_forever", 0),
                "median_2weeks": data.get("median_2weeks", 0),
                "price": data.get("price", 0),
                "tags": tags_str
            }
            
            logger.debug(f"Successfully fetched popularity stats for app ID {appid}")
            return stats
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error fetching popularity stats for app ID {appid}: {e}")
            return None
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing popularity stats for app ID {appid}: {e}")
            return None

    def close(self):
        """Close the requests session."""
        if self.session:
            self.session.close()
            logger.info("Steam API client session closed")
