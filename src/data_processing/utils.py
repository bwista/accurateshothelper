import requests
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_request(url, enable_logging=False):
    try:
        if enable_logging:
            logger.info(f"Making GET request to: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        if enable_logging:
            logger.debug("Request successful, returning JSON response")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        if enable_logging:
            logger.error(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.ConnectionError as conn_err:
        if enable_logging:
            logger.error(f"Connection error occurred: {conn_err}")  # Connection error
    except requests.exceptions.Timeout as timeout_err:
        if enable_logging:
            logger.error(f"Timeout error occurred: {timeout_err}")  # Timeout error
    except requests.exceptions.RequestException as req_err:
        if enable_logging:
            logger.error(f"Request error occurred: {req_err}")  # General request error
    except Exception as err:
        if enable_logging:
            logger.error(f"An unexpected error occurred: {err}", exc_info=True)  # Other errors with full traceback
    return None