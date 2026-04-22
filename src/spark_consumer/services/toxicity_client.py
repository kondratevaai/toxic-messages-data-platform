
import logging
import requests
from typing import Optional

# from src.logger import setup_logger
from logger import setup_logger


setup_logger()
logger = logging.getLogger(__name__)



class ToxicityClassifierClient:
    """Client for communicating with FastAPI toxicity classifier."""

    def __init__(self, base_url: str, token: Optional[str] = None, timeout: int = 30):
        """
        Initialize the classifier client.
        
        Args:
            base_url: Base URL of FastAPI service (e.g., 'http://backend-api:8000')
            token: Optional JWT/Bearer token for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()
        
        if token:
            self.session.headers.update({
                'Authorization': f'Bearer {token}' if not token.startswith('Bearer ') 
                                 else token
            })

    def authenticate(self) -> None:
        """
        Register a service user on the API and set the Bearer token.
        Skips if a token is already configured.
        """
        if self.token:
            return

        url = f"{self.base_url}/register"
        payload = {
            "name": "spark-consumer",
            "email": "spark-consumer@service.local",
        }
        try:
            resp = self.session.post(url, json=payload, timeout=self.timeout)
            if resp.status_code == 400 and "already exists" in resp.text:
                logger.info("Service user already registered, requesting new token")
                
                logger.warning(
                    "Cannot auto-authenticate: service user already exists. "
                    "Set 'api_token' in consumer_config.json."
                )
                return
            resp.raise_for_status()
            data = resp.json()
            self.token = data["access_token"]
            self.session.headers.update({
                'Authorization': f'Bearer {self.token}'
            })
            logger.info("Authenticated with API successfully")
        except Exception as e:
            logger.warning(f"Auto-authentication failed: {e}")
    
    def classify(self, text: str) -> dict:
        """
        Send text to classifier and get toxicity prediction.
        
        Args:
            text: Raw text to classify
            
        Returns:
            Dictionary with prediction results
            
        Raises:
            requests.RequestException: If API call fails
        """
        url = f"{self.base_url}/forward"
        payload = {"text_raw": text}
        
        try:
            response = self.session.post(
                url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise requests.RequestException(
                f"Request timeout after {self.timeout}s for text: {text[:100]}"
            )
        except requests.exceptions.ConnectionError as e:
            raise requests.RequestException(f"Connection error to {url}: {e}")
        except requests.exceptions.HTTPError as e:
            error_detail = response.text if hasattr(e, 'response') else str(e)
            raise requests.RequestException(
                f"HTTP error from {url}: {response.status_code} - {error_detail}"
            )
    
    def close(self):
        """Close the session."""
        self.session.close()