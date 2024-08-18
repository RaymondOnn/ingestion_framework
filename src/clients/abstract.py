


from pathlib import Path
import requests
from src.clients.base import Client


class O365Client(Client):
    def __init__(self, **kwargs):
        """
        Initializes a Sharepoint client with the given tenant ID, client ID,
        client secret, and resource URL.

        Args:
            tenant_id (str): The ID of the tenant.
            client_id (str): The client ID.
            client_secret (str): The client secret.
            resource_url (str): The resource URL.
        """
        super().__init__()
        self.tenant_id = kwargs.get("tenant_id", None)
        self.client_id = kwargs.get("client_id", None) 
        self.client_secret = kwargs.get("client_secret", None)
        self.resource_url = "https://graph.microsoft.com/v1.0"
        self.base_url = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )
        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.access_token = (
            self.get_access_token()
        )  # Initialize and store the access token upon instantiation
        self.headers = {"Authorization": f"Bearer {self.access_token}"}


    def get_access_token(self) -> str:
        """
        Retrieves an access token from Microsoft's OAuth2 endpoint.
        The access token is used to authenticate and authorize the application
        for accessing Microsoft Graph API resources.

        Returns:
            str: The access token as a string. This token is used for
                authentication in subsequent API requests.
        """
        # Body for the access token request
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "https://graph.microsoft.com/.default",
        }
        response = requests.post(
            self.base_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=data,
        )
        return response.json().get(
            "access_token"
        )  # Extract access token from the response
        
    def auto_renew_access_token(self):
        pass    
    
    def download_file(self, download_url, local_path, file_name) -> None:
        """
        Downloads a file from a given URL and saves it to a specified
        local path.

        Parameters:
            download_url (str): The URL from which the file will be downloaded.
            local_path (str): The local path where the file will be saved.
            file_name (str): The name of the file to be saved.

        Returns:
            None
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(download_url, headers=headers)
        if response.status_code == 200:
            full_path = Path(local_path) / file_name
            full_path = get_long_path(
                full_path
            )  # Apply the long path fix conditionally based on the OS
            ensure_directory_exists(full_path)
            with open(full_path, "wb") as file:
                file.write(response.content)
            print(f"File downloaded: {full_path}")
        else:
            print(
                f"Failed to download {file_name}: \
                    {response.status_code} - {response.reason}"
            )

    def _request(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except ConnectionError as conn_err:
            raise conn_err
        except requests.RequestException as req_err:
            raise req_err
        
    def _paginated_request(self, url): 
        results = []
        while url:
            try:
                results = self._request(url)
                results.extend(results.get("value")) 
                url = results.get("@odata.nextLink")
            except:
                break
        return results  

class DatabaseClient(Client):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    
    def load(self, path, **kwargs):
        pass
