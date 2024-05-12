"""Module to help with URL parsing and HTTP requests."""

# standard library
import logging
from urllib.parse import urlparse

# third party libraries
from google.auth.transport import requests as google_requests
import google.oauth2.id_token as token
import requests

# local libraries

class ParsedURL:
    """Class to hold URL components."""

    def __init__(self, url):
        """
        Initialize the ParsedURL class.

        Args:
            url (str): The URL to parse.
        """
        self.url = url
        self.components = urlparse(url)
        self.scheme = self.components.scheme
        self.netloc = self.components.netloc
        self.path = self.components.path
        self.query = self.components.query
        self.fragment = self.components.fragment
        self.base_url = f"{self.scheme}://{self.netloc}"

    def __repr__(self):
        """
        Return a string representation of the ParsedURL object.

        Returns:
            str: A string representation of the ParsedURL object.
        """
        return f"{self.scheme}://{self.netloc}{self.path}"

class GoogleAuthToken:
    """Class to generate Google authentication token."""

    def __init__(self, url):
        """
        Initialize the GoogleAuthToken class.
        
        Args:
            url (str): The URL to generate the request header for.
        """
        self.url = url
        self.components = ParsedURL(url)
    
    def generate_google_auth_token(self):
        """
        Generate a Google authentication token.

        Returns:
            str: The Google authentication token.
        """
        try:
            auth_request = google_requests.Request()
            return token.fetch_id_token(auth_request, self.components.base_url)
        except Exception as e:
            logging.error("Error generating Google auth token: %s", e)  

class GoogleAuthHeader:
    """Class to generate request header with Google authentication token."""

    def __init__(self, url):
        """
        Initialize the RequestHeaderWithGoogleAuthToken class.

        Args:
            url (str): The URL to generate the request header for.
        """
        self.url = url
        self.components = ParsedURL(url)
        self.auth_token = GoogleAuthToken(url).generate_google_auth_token()
    
    def generate_json_request_header_with_auth_token(self):
        """
        Generate a JSON request header with Google authentication token.

        Returns:
            dict: A dictionary containing the request header.
        """
        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json'
        }
        return headers