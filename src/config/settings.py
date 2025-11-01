"""
Configuration management for Stox application

Handles API keys, rate limits, and environment-specific settings with validation.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv


@dataclass
class RateLimitConfig:
    """Rate limiting configuration for API calls"""
    
    calls_per_minute: int
    calls_per_day: Optional[int] = None
    min_delay_seconds: float = 0.5
    
    def __post_init__(self):
        """Validate rate limit configuration"""
        if self.calls_per_minute <= 0:
            raise ValueError("calls_per_minute must be positive")
        if self.calls_per_day is not None and self.calls_per_day <= 0:
            raise ValueError("calls_per_day must be positive if specified")
        if self.min_delay_seconds < 0:
            raise ValueError("min_delay_seconds must be non-negative")


@dataclass
class APIConfig:
    """Configuration for a specific API"""
    
    name: str
    api_key: str
    base_url: str
    rate_limit: RateLimitConfig
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    enabled: bool = True
    
    def __post_init__(self):
        """Validate API configuration"""
        if not self.name:
            raise ValueError("API name is required")
        if not self.api_key:
            raise ValueError(f"API key is required for {self.name}")
        if not self.base_url:
            raise ValueError(f"Base URL is required for {self.name}")
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")


@dataclass
class DataConfig:
    """Configuration for data storage and management"""
    
    root_dir: Path
    cache_enabled: bool = True
    cache_ttl_hours: int = 24
    backup_enabled: bool = True
    compression: str = "snappy"
    default_format: str = "parquet"
    
    def __post_init__(self):
        """Validate data configuration"""
        if not isinstance(self.root_dir, Path):
            self.root_dir = Path(self.root_dir)
        if self.cache_ttl_hours < 0:
            raise ValueError("cache_ttl_hours must be non-negative")
        if self.default_format not in ["parquet", "csv", "json"]:
            raise ValueError("default_format must be one of: parquet, csv, json")


@dataclass
class Settings:
    """
    Main application settings
    
    Loads configuration from environment variables and provides
    validated access to all application settings.
    
    API configurations are stored in a dictionary for easy scalability.
    Access them via: settings.apis['alpha_vantage'] or settings.get_api('alpha_vantage')
    """
    
    apis: Dict[str, APIConfig]
    data: DataConfig
    
    # Application settings
    log_level: str = "INFO"
    log_dir: Path = field(default_factory=lambda: Path("logs"))
    
    def get_api(self, name: str) -> Optional[APIConfig]:
        """
        Get API configuration by name (case-insensitive)
        
        Args:
            name: API name (e.g., 'alpha_vantage', 'AlphaVantage', 'ALPHA_VANTAGE')
            
        Returns:
            APIConfig if found, None otherwise
        """
        # Normalize name to lowercase with underscores
        normalized = name.lower().replace('-', '_').replace(' ', '_')
        return self.apis.get(normalized)
    
    @classmethod
    def _load_api_config(
        cls,
        api_name: str,
        required: bool = True,
        defaults: Optional[Dict[str, Any]] = None
    ) -> Optional[APIConfig]:
        """
        Load a single API configuration from environment variables
        
        Environment variable pattern:
        - {API_NAME}_API_KEY
        - {API_NAME}_BASE_URL
        - {API_NAME}_CALLS_PER_MINUTE
        - {API_NAME}_CALLS_PER_DAY
        - {API_NAME}_MIN_DELAY
        - {API_NAME}_TIMEOUT
        - {API_NAME}_MAX_RETRIES
        - {API_NAME}_ENABLED
        
        Args:
            api_name: Name of the API (e.g., 'ALPHAVANTAGE', 'YAHOO', 'MASSIVE')
            required: Whether this API is required (raises error if missing)
            defaults: Dictionary of default values
            
        Returns:
            APIConfig instance or None if not required and not configured
            
        Raises:
            ValueError: If required API key is missing
        """
        defaults = defaults or {}
        prefix = api_name.upper()
        
        # Check for API key
        api_key = os.getenv(f"{prefix}_API_KEY", defaults.get("api_key", ""))
        
        if required and not api_key:
            raise ValueError(f"{prefix}_API_KEY environment variable is required")
        
        # If not required and no key, return None
        if not required and not api_key:
            return None
        
        # Load configuration with defaults
        base_url = os.getenv(f"{prefix}_BASE_URL", defaults.get("base_url", ""))
        if not base_url:
            raise ValueError(f"{prefix}_BASE_URL is required")
        
        rate_limit = RateLimitConfig(
            calls_per_minute=int(os.getenv(
                f"{prefix}_CALLS_PER_MINUTE",
                str(defaults.get("calls_per_minute", 60))
            )),
            calls_per_day=int(os.getenv(
                f"{prefix}_CALLS_PER_DAY",
                str(defaults.get("calls_per_day", 0))
            )) or None,  # Convert 0 to None
            min_delay_seconds=float(os.getenv(
                f"{prefix}_MIN_DELAY",
                str(defaults.get("min_delay_seconds", 0.5))
            ))
        )
        
        return APIConfig(
            name=defaults.get("display_name", api_name.title()),
            api_key=api_key,
            base_url=base_url,
            rate_limit=rate_limit,
            timeout=int(os.getenv(
                f"{prefix}_TIMEOUT",
                str(defaults.get("timeout", 30))
            )),
            max_retries=int(os.getenv(
                f"{prefix}_MAX_RETRIES",
                str(defaults.get("max_retries", 3))
            )),
            enabled=os.getenv(
                f"{prefix}_ENABLED",
                str(defaults.get("enabled", True))
            ).lower() == "true"
        )
    
    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> "Settings":
        """
        Load settings from environment variables
        
        To add a new API source, simply add environment variables with the pattern:
        - {API_NAME}_API_KEY
        - {API_NAME}_BASE_URL
        - {API_NAME}_CALLS_PER_MINUTE (optional)
        - etc.
        
        Args:
            env_file: Optional path to .env file
            
        Returns:
            Settings instance with validated configuration
            
        Raises:
            ValueError: If required environment variables are missing or invalid
        """
        # Load environment variables
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()
        
        # Dictionary to store all API configurations
        apis = {}
        
        # Alpha Vantage configuration
        # NOTE: Free tier = 5 calls/min, 500 calls/day
        #       Premium tier = 75+ calls/min, unlimited daily
        #       Adjust ALPHAVANTAGE_CALLS_PER_MINUTE in .env for your subscription
        alpha_vantage = cls._load_api_config(
            "ALPHAVANTAGE",
            required=True,
            defaults={
                "display_name": "AlphaVantage",
                "base_url": "https://www.alphavantage.co/query",
                "calls_per_minute": 75,  # Free tier default
                "calls_per_day": 500,
                "min_delay_seconds": 0.8,  # 60s / 5 calls
                "timeout": 30,
                "max_retries": 3,
                "enabled": True
            }
        )
        if alpha_vantage:
            apis["alpha_vantage"] = alpha_vantage
        
        # Yahoo Finance configuration
        # NOTE: No API key required, generally unlimited for personal use
        yahoo_finance = cls._load_api_config(
            "YAHOO",
            required=False,  # Optional since it has no API key requirement
            defaults={
                "display_name": "YahooFinance",
                "api_key": "not_required",  # Placeholder
                "base_url": "https://query2.finance.yahoo.com",
                "calls_per_minute": 60,
                "min_delay_seconds": 0.5,
                "timeout": 30,
                "max_retries": 3,
                "enabled": True
            }
        )
        if yahoo_finance:
            apis["yahoo_finance"] = yahoo_finance
        
        # Massive configuration  
        # NOTE: Rate limits depend on subscription tier
        massive = cls._load_api_config(
            "MASSIVE",
            required=True,
            defaults={
                "display_name": "Massive",
                "base_url": "https://api.massive.com/v3",
                "calls_per_minute": 60,
                "min_delay_seconds": 0.5,
                "timeout": 30,
                "max_retries": 3,
                "enabled": True
            }
        )
        if massive:
            apis["massive"] = massive
        
        # Auto-discover additional API configurations
        # Look for any environment variables matching pattern {NAME}_API_KEY
        # that haven't been explicitly configured above
        known_apis = {"alphavantage", "yahoo", "massive"}
        for key in os.environ:
            if key.endswith("_API_KEY"):
                api_prefix = key[:-8]  # Remove '_API_KEY'
                if api_prefix.lower() not in known_apis:
                    # Try to load this API dynamically
                    try:
                        config = cls._load_api_config(
                            api_prefix,
                            required=False,
                            defaults={
                                "display_name": api_prefix.replace("_", " ").title(),
                                "base_url": os.getenv(f"{api_prefix}_BASE_URL", ""),
                            }
                        )
                        if config:
                            apis[api_prefix.lower()] = config
                    except ValueError:
                        # Skip if BASE_URL is missing or other validation fails
                        pass
        
        # Data configuration
        data_root = Path(os.getenv("DATA_ROOT_DIR", "data"))
        data = DataConfig(
            root_dir=data_root,
            cache_enabled=os.getenv("CACHE_ENABLED", "true").lower() == "true",
            cache_ttl_hours=int(os.getenv("CACHE_TTL_HOURS", "24")),
            backup_enabled=os.getenv("BACKUP_ENABLED", "true").lower() == "true",
            compression=os.getenv("DATA_COMPRESSION", "snappy"),
            default_format=os.getenv("DATA_DEFAULT_FORMAT", "parquet")
        )
        
        # Application settings
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        log_dir = Path(os.getenv("LOG_DIR", "logs"))
        
        return cls(
            apis=apis,
            data=data,
            log_level=log_level,
            log_dir=log_dir
        )
    
    def validate(self) -> None:
        """
        Validate all configuration settings
        
        Raises:
            ValueError: If any configuration is invalid
        """
        # Validate directories exist or can be created
        for dir_path in [self.data.root_dir, self.log_dir]:
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise ValueError(f"Cannot create directory {dir_path}: {e}")
        
        # Additional validation can be added here
        if self.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(
                f"Invalid LOG_LEVEL: {self.log_level}. "
                "Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL"
            )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert settings to dictionary (for debugging/logging)
        
        Note: API keys are masked for security
        
        Returns:
            Dictionary representation of settings
        """
        def mask_api_key(key: str) -> str:
            """Mask API key for logging"""
            if not key or len(key) < 8:
                return "***"
            return f"{key[:4]}...{key[-4:]}"
        
        # Build APIs dictionary
        apis_dict = {}
        for api_name, api_config in self.apis.items():
            apis_dict[api_name] = {
                "name": api_config.name,
                "api_key": mask_api_key(api_config.api_key),
                "base_url": api_config.base_url,
                "enabled": api_config.enabled,
                "rate_limit": {
                    "calls_per_minute": api_config.rate_limit.calls_per_minute,
                    "calls_per_day": api_config.rate_limit.calls_per_day
                }
            }
        
        return {
            "apis": apis_dict,
            "data": {
                "root_dir": str(self.data.root_dir),
                "cache_enabled": self.data.cache_enabled,
                "backup_enabled": self.data.backup_enabled,
                "default_format": self.data.default_format
            },
            "log_level": self.log_level,
            "log_dir": str(self.log_dir)
        }


_settings: Optional[Settings] = None


def get_settings(reload: bool = False) -> Settings:
    """
    Get the application settings singleton
    
    Args:
        reload: Force reload settings from environment
        
    Returns:
        Settings instance
    """
    global _settings
    
    if _settings is None or reload:
        _settings = Settings.from_env()
        _settings.validate()
    
    return _settings
