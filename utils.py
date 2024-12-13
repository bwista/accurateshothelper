import requests

def get_request(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")  # Connection error
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")  # Timeout error
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")  # General request error
    except Exception as err:
        print(f"An error occurred: {err}")  # Other errors
    return None