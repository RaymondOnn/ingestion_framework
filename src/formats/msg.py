from dataclasses import dataclass

import extract_msg


class ParseMessageError(Exception):
    pass


class BaseProcessor:
    def read(self, file_path: str):

        pass


@dataclass
class Msg:
    id: int
    email: str
    sender: str
    recipients: list[str]
    cc: list[str]
    bcc: list[str]
    timestamp: int


class MsgProcessor:
    def __init__(self, msg_file_path: str) -> None:
        self.msg = extract_msg.Message(msg_file_path)

    def get_sender(self) -> str:
        return self.msg.sender

    def get_recipients(self) -> list[str]:
        return self.msg.recipients
