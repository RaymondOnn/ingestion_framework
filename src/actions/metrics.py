
from dataclasses import dataclass, field
from datetime import datetime



# class Failure:

#     def __init__(self, message: str):
#         self.message = message
        
# class Success:

#     def __init__(self, message: str):
#         self.message = message  

@dataclass
class Metrics:
    start_time: datetime
    end_time: datetime
    memory_used: str
    files_read: int = field(default=0)
    files_written: int = field(default=0)
    errors: list[Exception] = field(default_factory=list)
