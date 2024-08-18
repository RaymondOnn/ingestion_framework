import os
import platform
from typing import Any
from typing import Optional

import requests

from src.clients.abstract import O365Client

class Outlook(O365Client):
    def __init__(self, **kwargs) -> None:

        tenant_id = kwargs.get("tenant_id", None)
        client_id = kwargs.get("client_id", None) 
        client_secret = kwargs.get("client_secret", None)
        super().__init__(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        
    def get_user_id(self, user_email: str) -> str:
        url = f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{user_email}'"
        response = requests.get(url, headers=self.headers)
        users = response.json().get("value", [])
        if len(users) > 0:
            return users[0]["id"]
        else:
            raise ValueError(f"User '{user_email}' not found.")
        
    
    def get_emails(
        self, 
        user_id: str, 
        email_subject: Optional[str] = None, 
        after_ts: Optional[str] = None,
        before_ts: Optional[str] = None,
    ) -> str:
        url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages"
        params = {
            "$filter": f"subject eq '{email_subject}'",
            "$orderby": "receivedDateTime desc",
            "$top": "10",
            "$select": "subject,receivedDateTime",
            "$count": "true",
        }
        response = requests.get(url, headers=self.headers, params=params)
        return response.json()
    
    def get_message_contents(self, user_id, message_id) -> str:
        url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}"
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def download_attachments(self, user_id, message_id, local_path) -> bool:
        url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/attachments"
        response = requests.get(url, headers=self.headers)
        attachments = response.json()
        
        if "value" in response.json():
            attachments = response.json().get("value", [])
            for attachment in attachments:
                attachment_id = attachment["id"]
                attachment_name = attachment["name"]
                # download_url = attachment["contentUrl"]
                download_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/attachments/{attachment_id}/$value"
                self.download_file(download_url, local_path, attachment_name)
                
        return True