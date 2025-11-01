"""
Base API functionality for all external API wrappers

Provides common utilities for rate limiting, retries, error handling, and logging.
Uses composition over strict inheritance - API classes can use these utilities as needed.
"""

import time
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Callable, TypeVar
from datetime import datetime, timedelta
from functools import wraps
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import APIConfig, RateLimitConfig


# Type variable for generic return types
T = TypeVar('T')


class RateLimiter:
    """
    Rate limiter that enforces API rate limits
    
    Tracks calls per minute and per day, ensuring compliance with API limits.
    """
    
    def __init__(self, config: RateLimitConfig):
        """
        Initialize rate limiter
        
        Args:
            config: Rate limit configuration
        """
        self.config = config
        self.minute_calls: list[float] = []
        self.day_calls: list[float] = []
        self.last_call_time: Optional[float] = None
        
        self.logger = logging.getLogger(f"{__name__}.RateLimiter")
    
    def wait_if_needed(self) -> None:
        """
        Wait if necessary to comply with rate limits
        
        Checks both per-minute and per-day limits, and enforces minimum delay.
        """
        current_time = time.time()
        
        # Enforce minimum delay between calls
        if self.last_call_time:
            time_since_last = current_time - self.last_call_time
            if time_since_last < self.config.min_delay_seconds:
                wait_time = self.config.min_delay_seconds - time_since_last
                self.logger.debug(f"Waiting {wait_time:.2f}s for minimum delay")
                time.sleep(wait_time)
                current_time = time.time()
        
        # Clean up old minute calls (keep only last 60 seconds)
        minute_ago = current_time - 60
        self.minute_calls = [t for t in self.minute_calls if t > minute_ago]
        
        # Check per-minute limit
        if len(self.minute_calls) >= self.config.calls_per_minute:
            # Need to wait until the oldest call falls out of the window
            oldest_call = self.minute_calls[0]
            wait_time = 60 - (current_time - oldest_call) + 0.1  # Add small buffer
            if wait_time > 0:
                self.logger.info(
                    f"Rate limit: {len(self.minute_calls)}/{self.config.calls_per_minute} "
                    f"calls in last minute. Waiting {wait_time:.1f}s"
                )
                time.sleep(wait_time)
                current_time = time.time()
                # Clean up again after waiting
                minute_ago = current_time - 60
                self.minute_calls = [t for t in self.minute_calls if t > minute_ago]
        
        # Check per-day limit (if configured)
        if self.config.calls_per_day:
            # Clean up old day calls (keep only last 24 hours)
            day_ago = current_time - (24 * 60 * 60)
            self.day_calls = [t for t in self.day_calls if t > day_ago]
            
            if len(self.day_calls) >= self.config.calls_per_day:
                # Calculate wait time until oldest call falls out of 24h window
                oldest_call = self.day_calls[0]
                wait_time = (24 * 60 * 60) - (current_time - oldest_call) + 1
                self.logger.warning(
                    f"Daily rate limit reached: {len(self.day_calls)}/{self.config.calls_per_day} "
                    f"calls in last 24h. Would need to wait {wait_time/3600:.1f} hours"
                )
                raise RateLimitException(
                    f"Daily rate limit of {self.config.calls_per_day} calls exceeded. "
                    f"Wait {wait_time/3600:.1f} hours or upgrade your plan."
                )
    
    def record_call(self) -> None:
        """Record that an API call was made"""
        current_time = time.time()
        self.minute_calls.append(current_time)
        if self.config.calls_per_day:
            self.day_calls.append(current_time)
        self.last_call_time = current_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current rate limiter statistics"""
        current_time = time.time()
        minute_ago = current_time - 60
        day_ago = current_time - (24 * 60 * 60)
        
        minute_calls = [t for t in self.minute_calls if t > minute_ago]
        day_calls = [t for t in self.day_calls if t > day_ago]
        
        return {
            "calls_last_minute": len(minute_calls),
            "limit_per_minute": self.config.calls_per_minute,
            "calls_last_day": len(day_calls),
            "limit_per_day": self.config.calls_per_day,
            "last_call_ago_seconds": current_time - self.last_call_time if self.last_call_time else None
        }


class RateLimitException(Exception):
    """Exception raised when rate limit is exceeded"""
    pass


class APIException(Exception):
    """Base exception for API-related errors"""
    pass


class BaseAPI(ABC):
    """
    Abstract base class for API wrappers
    
    Provides common functionality for making API requests with:
    - Automatic rate limiting
    - Retry logic with exponential backoff
    - Error handling and logging
    - Session management with connection pooling
    """
    
    def __init__(self, config: APIConfig):
        """
        Initialize base API
        
        Args:
            config: API configuration
        """
        self.config = config
        self.rate_limiter = RateLimiter(config.rate_limit)
        self.logger = logging.getLogger(f"{__name__}.{config.name}")
        
        # Create session with retry logic
        self.session = self._create_session()
        
        # Statistics
        self.stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "retried_calls": 0,
            "total_wait_time": 0.0
        }
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry logic and connection pooling
        
        Returns:
            Configured requests.Session
        """
        session = requests.Session()
        
        # Configure retries for connection errors and server errors (5xx)
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make an HTTP request with rate limiting and error handling
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: URL parameters
            data: Form data
            json: JSON data
            headers: HTTP headers
            **kwargs: Additional arguments for requests
            
        Returns:
            requests.Response object
            
        Raises:
            RateLimitException: If rate limit is exceeded
            APIException: If API returns an error
        """
        if not self.config.enabled:
            raise APIException(f"API {self.config.name} is disabled in configuration")
        
        # Wait for rate limiting
        start_wait = time.time()
        self.rate_limiter.wait_if_needed()
        wait_time = time.time() - start_wait
        self.stats["total_wait_time"] += wait_time
        
        # Add API key to request if needed
        if params is None:
            params = {}
        if self.config.api_key and self._should_add_api_key_to_params():
            params = {**params, **self._get_api_key_params()}
        
        # Merge headers
        request_headers = self._get_default_headers()
        if headers:
            request_headers.update(headers)
        
        # Make request
        try:
            self.logger.debug(f"{method} {url} with params={params}")
            
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=request_headers,
                timeout=self.config.timeout,
                **kwargs
            )
            
            # Record successful call
            self.rate_limiter.record_call()
            self.stats["total_calls"] += 1
            
            # Check response
            if response.status_code >= 400:
                self.stats["failed_calls"] += 1
                error_msg = f"API error {response.status_code}: {response.text[:200]}"
                self.logger.error(error_msg)
                raise APIException(error_msg)
            
            self.stats["successful_calls"] += 1
            self.logger.debug(f"Success: {response.status_code}")
            
            return response
            
        except requests.exceptions.Timeout:
            self.stats["failed_calls"] += 1
            error_msg = f"Request timeout after {self.config.timeout}s"
            self.logger.error(error_msg)
            raise APIException(error_msg)
        
        except requests.exceptions.RequestException as e:
            self.stats["failed_calls"] += 1
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(error_msg)
            raise APIException(error_msg)
    
    def _should_add_api_key_to_params(self) -> bool:
        """
        Check if API key should be added to request parameters
        
        Override this in subclasses if API key is added differently
        (e.g., in headers, auth, etc.)
        
        Returns:
            True if API key should be added to params
        """
        return True
    
    def _get_api_key_params(self) -> Dict[str, str]:
        """
        Get API key as URL parameters
        
        Override this in subclasses to customize API key parameter name
        
        Returns:
            Dictionary with API key parameter
        """
        return {"apikey": self.config.api_key}
    
    def _get_default_headers(self) -> Dict[str, str]:
        """
        Get default HTTP headers
        
        Override this in subclasses to add custom headers
        
        Returns:
            Dictionary of HTTP headers
        """
        return {
            "User-Agent": f"Stox/1.0 ({self.config.name})",
            "Accept": "application/json"
        }
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make a GET request"""
        return self._make_request("GET", url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Make a POST request"""
        return self._make_request("POST", url, **kwargs)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get API usage statistics
        
        Returns:
            Dictionary with usage stats
        """
        return {
            **self.stats,
            "rate_limiter": self.rate_limiter.get_stats(),
            "config": {
                "name": self.config.name,
                "enabled": self.config.enabled,
                "calls_per_minute": self.config.rate_limit.calls_per_minute,
                "calls_per_day": self.config.rate_limit.calls_per_day
            }
        }
    
    def close(self) -> None:
        """Close the session and clean up resources"""
        if self.session:
            self.session.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test if the API connection is working
        
        Subclasses should implement this to verify API credentials and connectivity.
        
        Returns:
            True if connection is successful, False otherwise
        """
        pass


def rate_limited(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to add rate limiting to a function
    
    The function must be a method of a class that has a `rate_limiter` attribute.
    
    Usage:
        @rate_limited
        def fetch_data(self):
            # Your code here
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs) -> T:
        if hasattr(self, 'rate_limiter'):
            self.rate_limiter.wait_if_needed()
            result = func(self, *args, **kwargs)
            self.rate_limiter.record_call()
            return result
        else:
            # If no rate limiter, just call the function
            return func(self, *args, **kwargs)
    
    return wrapper
