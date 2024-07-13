import os
import platform
from typing import Any
from typing import Optional

import requests


def ensure_directory_exists(file_path):
    """
    A function that ensures the existence of the directory containing the
    specified file path.

    Parameters:
        file_path (str): The path of the file for which to ensure the
            directory existence.

    Returns:
        None
    """
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def get_long_path(path: str) -> str:
    """
    Returns the long path for the given `path` on Windows systems, and the
    normal path for Unix-based systems.

    Parameters:
        path (str): The path for which to get the long path.

    Returns:
        str: The long path on Windows systems, or the normal path on
            Unix-based systems.
    """
    # Check if the operating system is Windows
    if platform.system() == "Windows":
        # Apply the \\?\ prefix correctly to handle long paths on Windows
        return "\\\\?\\" + os.path.abspath(path).strip()
    else:
        # Return the normal path for Unix-based systems
        return os.path.abspath(path)


class Sharepoint:
    """
    This class represents a client for interacting with SharePoint.
    It provides methods for authenticating with Microsoft Graph API,
    retrieving site and drive IDs, listing folder contents, downloading files,
    and recursively downloading files from a folder and its subfolders.
    """

    def __init__(
        self,
        tenant_id,
        client_id,
        client_secret,
        resource_url,
    ) -> None:
        """
        Initializes a Sharepoint client with the given tenant ID, client ID,
        client secret, and resource URL.

        Args:
            tenant_id (str): The ID of the tenant.
            client_id (str): The client ID.
            client_secret (str): The client secret.
            resource_url (str): The resource URL.
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.resource_url = resource_url
        self.base_url = (
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        )
        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.access_token = (
            self.get_access_token()
        )  # Initialize and store the access token upon instantiation

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
            "scope": self.resource_url + ".default",
        }
        response = requests.post(
            self.base_url,
            headers=self.headers,
            data=data,
        )
        return response.json().get(
            "access_token"
        )  # Extract access token from the response

    def get_site_id(self, site_url) -> str:
        """
        Retrieves the site ID for a given SharePoint site using the
        Microsoft Graph API.

        Args:
            site_url (str): The URL of the SharePoint site.

        Returns:
            str: The ID of the SharePoint site.
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)
        return response.json().get("id")  # Return the site ID

    def get_drive_id(self, site_id) -> list[dict[str, str]]:
        """
        Retrieves the IDs and names of all drives associated with a specified
        SharePoint site.

        Args:
            site_id (str): The ID of the SharePoint site.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing the
                drive ID and name.
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)
        drives = response.json().get("value", [])
        return [
            ({"id": drive["id"], "name": drive["name"]}) for drive in drives
        ]  # noqa

    def get_folder_id(self, site_id, drive_id, folder_path) -> Optional[str]:
        """
        Retrieves the folder ID for a given folder path within a site
        and drive.

        Args:
            site_id (str): The ID of the site.
            drive_id (str): The ID of the drive.
            folder_path (str): The path of the folder.

        Returns:
            str: The ID of the folder. Returns None if the folder is not found.
        """
        # Split the folder path into individual folders
        folders = folder_path.split("/")

        # Start with the root folder
        current_folder_id = "root"

        # Loop through each folder in the path
        for folder_name in folders:
            # Build the URL to access the contents of the current folder
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{current_folder_id}/children"  # noqa
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(url, headers=headers)
            items_data = response.json()

            # Loop through the items and find the folder
            for item in items_data["value"]:
                if "folder" in item and item["name"] == folder_name:
                    # Update the current folder ID and break the loop
                    current_folder_id = item["id"]
                    break
            else:
                # If the folder was not found, return None
                return None

        # Return the ID of the final folder in the path
        return current_folder_id

    def list_folder_contents(
        self, site_id, drive_id, folder_id="root"
    ) -> list[dict[str, Any]]:
        """
        Lists the contents of a folder within a site and drive.

        Args:
            site_id (str): The ID of the site.
            drive_id (str): The ID of the drive.
            folder_id (str, optional): The ID of the folder.
                Defaults to 'root'.

        Returns:
            List[Dict[str, Union[str, Dict[str, str]]]]: A list of
                dictionaries containing information about each item in
                the folder.
        """
        items_list = []
        folder_contents_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{folder_id}/children"  # noqa
        while folder_contents_url:
            contents_response = requests.get(
                folder_contents_url,
                headers={"Authorization": f"Bearer {self.access_token}"},
            )
            folder_contents = contents_response.json()
            for item in folder_contents.get("value", []):
                path_parts = item["parentReference"]["path"].split("root:")
                path = path_parts[1] if len(path_parts) > 1 else ""
                full_path = f"{path}/{item['name']}" if path else item["name"]

                # Get the web URL
                item_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item["id"]}'  # noqa
                response = requests.get(
                    item_url,
                    headers={"Authorization": f"Bearer {self.access_token}"},
                )
                item_data = response.json()
                item_web_url = item_data.get("webUrl", "")

                items_list.append(
                    {
                        "id": item["id"],
                        "name": item["name"],
                        "type": "folder" if "folder" in item else "file",
                        "mimeType": item["file"]["mimeType"]
                        if "file" in item
                        else "",  # noqa
                        "uri": item.get("@microsoft.graph.downloadUrl", ""),
                        "path": path,
                        "fullpath": full_path,
                        "filename": item["name"],
                        "url": item_web_url,
                    }
                )
            folder_contents_url = folder_contents.get("@odata.nextLink")
        return items_list

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
            full_path = os.path.join(local_path, file_name)
            full_path = get_long_path(
                full_path
            )  # Apply the long path fix conditionally based on the OS
            ensure_directory_exists(full_path)
            with open(full_path, "wb") as file:
                file.write(response.content)
            # print(f"File downloaded: {full_path}")
        else:
            print(
                f"Failed to download {file_name}: \
                    {response.status_code} - {response.reason}"
            )

    def download_folder_contents(
        self, site_id, drive_id, folder_id, local_folder_path, level=0
    ) -> None:
        """
        Recursively downloads all contents from a folder in SharePoint.

        Args:
            site_id (str): The ID of the SharePoint site.
            drive_id (str): The ID of the drive on the SharePoint site.
            folder_id (str): The ID of the folder to download contents from.
            local_folder_path (str): The local path where the downloaded
                contents will be saved.
            level (int, optional): The recursion level. Defaults to 0.

        Returns:
            None
        """
        # Recursively download all contents from a folder
        folder_contents_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{folder_id}/children"  # noqa
        contents_headers = {"Authorization": f"Bearer {self.access_token}"}
        contents_response = requests.get(
            folder_contents_url,
            headers=contents_headers,
        )
        folder_contents = contents_response.json()

        if "value" in folder_contents:
            for item in folder_contents["value"]:
                if "folder" in item:
                    new_path = os.path.join(local_folder_path, item["name"])
                    if not os.path.exists(new_path):
                        os.makedirs(new_path)
                    # Recursive call for subfolders
                    self.download_folder_contents(
                        site_id, drive_id, item["id"], new_path, level + 1
                    )
                elif "file" in item:
                    file_name = item["name"]
                    file_download_url = f"{self.resource_url}/v1.0/sites/{site_id}/drives/{drive_id}/items/{item['id']}/content"  # noqa
                    self.download_file(
                        file_download_url,
                        local_folder_path,
                        file_name,
                    )

    def download_file_contents(
        self, site_id, drive_id, file_id, local_save_path
    ) -> bool:
        """
        Downloads the contents of a file from a SharePoint site.

        Args:
            site_id (str): The ID of the SharePoint site.
            drive_id (str): The ID of the drive containing the file.
            file_id (str): The ID of the file to download.
            local_save_path (str): The local path to save the downloaded file.

        Returns:
            bool: True if the file was successfully downloaded,
                False otherwise.

        Raises:
            requests.exceptions.RequestException: If there was an error making
                the request to the SharePoint API.

        Description:
            This function downloads the contents of a file from a
            SharePoint site. It first retrieves the file details using the
            SharePoint API. It then extracts the download URL and file name
            from the file details. It also extracts the SharePoint file path
            and creates a local sub-folder if necessary.
            Finally, it downloads the file using the download URL and saves it
            to the specified local path.

            If the file download is successful, the function returns True.
            If there is an error making the request to the SharePoint API,
            the function raises a `requests.exceptions.RequestException`
            exception and returns False.
        """
        try:
            # Get the file details
            file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}"  # noqa
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(file_url, headers=headers)
            file_data = response.json()

            # Get the download URL and file name
            download_url = file_data["@microsoft.graph.downloadUrl"]
            file_name = file_data["name"]
            sharepoint_file_path = file_data["parentReference"][
                "path"
            ]  # This is the SharePoint file path
            index = sharepoint_file_path.find(":/")

            # Extract everything after ":/"
            if index != -1:
                # Adding 2 to skip the characters ":/"
                extracted_path = sharepoint_file_path[index + 2 :]  # noqa
                local_save_path = local_save_path + "/" + extracted_path

                # create local sub-folder
                os.makedirs(local_save_path, exist_ok=True)
            else:
                extracted_path = ""
            # print(f"Downloading {file_name} from {extracted_path}")

            # Download the file
            self.download_file(download_url, local_save_path, file_name)

            # If no exception was raised, the file download was successful
            return True

        except requests.exceptions.RequestException as e:
            print(f"Error downloading file: {file_name} err: {e}")
            return False

    def download_all_files(
        self,
        site_id,
        drive_id,
        local_folder_path,
        sharepoint_path: str = "root",
    ) -> None:
        """
        Downloads all files from a SharePoint site recursively starting from
        a specified folder.

        Args:
            site_id (str): The ID of the SharePoint site.
            drive_id (str): The ID of the drive containing the files.
            local_folder_path (str): The local path to store the downloaded
                files.
            sharepoint_path (str, optional): The path in the SharePoint site
                to start downloading files from. Defaults to "root".
        """
        try:
            if sharepoint_path != "root":
                folder_id = self.get_folder_id(
                    site_id,
                    drive_id,
                    sharepoint_path,
                )
            else:
                folder_id = sharepoint_path

            self.recursive_download(
                site_id,
                drive_id,
                folder_id,
                local_folder_path,
            )
        except Exception as e:
            print(f"An error occurred while downloading files: {e}")

    def recursive_download(
        self, site_id: str, drive_id: str, folder_id: str, local_path: str
    ) -> None:
        """
        This method downloads files from a folder and its subfolders
        recursively.

        Args:
            site_id (str): The ID of the site from which files are to be
                downloaded.
            drive_id (str): The ID of the drive on the site from which files
                are to be downloaded.
            folder_id (str): The ID of the folder from which files are to be
                downloaded.
            local_path (str): The local path where the downloaded files should
                be stored.

        Raises:
            Exception: If an error occurs while recursively downloading files.

        Description:
            This function downloads files from a folder and its subfolders
                recursively.
            It first retrieves the contents of the specified folder using
                the `list_folder_contents` method.
            Then, for each item in the folder contents, it checks if it is a
                folder or a file.
            If it is a folder, it recursively calls the `recursive_download`
                method to download files from the subfolder.
            If it is a file, it downloads the file using the `download_file`
                method and saves it to the specified local path.
            If an error occurs during the recursive download process, an
                exception is raised with the error message and the path of the
                file that caused the error.
        """
        try:
            folder_contents = self.list_folder_contents(
                site_id,
                drive_id,
                folder_id,
            )
            for item in folder_contents:
                sharepoint_path = item["path"]
                sharepoint_path = sharepoint_path.lstrip("/")
                new_local_path = os.path.normpath(
                    os.path.join(local_path, sharepoint_path)
                )
                # Ensure the local directory exists before downloading
                # "data2\\BASIC STUDIES\\1. Cross Category
                # os.makedirs(
                # "data2\\BASIC STUDIES\\1. Cross Category",
                # exist_ok=True)

                os.makedirs(new_local_path, exist_ok=True)
                if item["type"] == "folder":
                    self.recursive_download(
                        site_id,
                        drive_id,
                        item["id"],
                        local_path,
                    )
                elif item["type"] == "file":
                    # os.makedirs(
                    # os.path.dirname(new_local_path),
                    # exist_ok=True,
                    # )
                    self.download_file(
                        item["uri"],
                        new_local_path,
                        item["name"],
                    )
        except Exception as e:
            print(
                f"An error occurred while recursively downloading files:\
                {new_local_path} {e}"
            )
