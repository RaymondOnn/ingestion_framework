import random
import shutil
import string
from pathlib import Path
from typing import Any
from typing import Self

from watchdog.events import FileSystemEvent
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.utils.classes import Singleton
from src.utils.log import logger

DEFAULT_PARENT_FOLDER = str(Path().cwd())

# class Watcher:
#     def __init__(self, path):
#         self.observer = Observer()
#         self.path = path

#     def run(self):
#         event_handler = Handler()
#         self.observer.schedule(event_handler, self.path, recursive=True)
#         self.observer.start()
#         try:
#             while True:
#                 time.sleep(1)
#         except:
#             self.observer.stop()
#             print("Error")

#         self.observer.join()


class FileEventHandler(FileSystemEventHandler):
    # def on_created(self, event):
    #     dir_path = event.src_path.split('/input_files')
    #     processed_files = f'{dir_path[0]}/processed_files'

    #     child_processed_dir = create_directory(file_path=processed_files)

    #     if event:
    #         print("file created:{}".format(event.src_path))
    #         # call function here
    #         main(file_name=event.src_path)

    #         file_name = event.src_path.split('/')[-1]
    #         destination_path = f'{child_processed_dir}/{file_name}'

    #         shutil.move(event.src_path, destination_path)
    #         print("file moved:{} to {}".format(event.src_path, destination_path))  # noqa

    @staticmethod
    def on_any_event(event):
        path = Path(event.src_path).relative_to(Path().cwd())
        if all([event.event_type not in ["created", "modified"]]):
            logger.debug(f"File {event.event_type}: % s." % path)
        elif all([event.event_type == "modified", not event.is_directory]):
            pass
            # logger.debug(f"File {event.event_type}: % s." % path)

    @staticmethod
    def on_created(event: FileSystemEvent) -> None:
        if all(
            ["outputs" in str(event.src_path).lower(), not event.is_directory]
        ):  # noqa
            # Event is created, you can process it now
            logger.debug(f"File created: {event.src_path}")
            # Outputs.log(task='step', key='key', value='text.txt')


class WorkingDirectory:
    def __init__(
        self,
        parent_directory: str,
        directory_name: str,
        is_temp: bool,
        make_output_dir: bool = True,
    ) -> None:
        self.is_temp = is_temp
        self.object = Path(parent_directory) / directory_name
        logger.success(
            f"Created Temporary working directory:{str(self.object)}"
        )  # noqa
        if make_output_dir:
            self.mkdir("outputs")
        self.__monitor(recursive=True)

    def __monitor(self, recursive: bool = True) -> None:
        self.observer = Observer()
        self.event_handler = FileEventHandler()
        self.observer.schedule(
            self.event_handler, str(self.object), recursive=recursive
        )
        self.observer.start()

    @classmethod
    def create(
        cls,
        directory_name: str | None = None,
        parent_directory: str | None = None,
        is_temp: bool = False,
    ) -> Self:
        if directory_name is None:
            random_str = ""
            for _ in range(8):
                random_str += random.SystemRandom().choice(
                    string.ascii_letters + string.digits
                )
            directory_name = "tmp" + "".join(random_str)
        parent_directory = parent_directory or DEFAULT_PARENT_FOLDER
        return cls(parent_directory, directory_name, is_temp)

    def mkdir(self, subfolder_name: str) -> None:
        self.output_dir: Path = self.object.joinpath(subfolder_name)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def remove(self) -> None:
        self.observer.stop()
        shutil.rmtree(self.object)


class Outputs(Singleton):
    data: dict[str, Any] = {}
    # def __init__(self, dir_path: str) -> None:
    #     self.dir_path = dir_path

    @classmethod
    def log(cls, task, key, value):
        # cls.data = cls.data or {}
        cls.data.update({f"{task}#{key}": value})
        logger.info(f"Artifact Logged: {task}#{key} = {value}")

    @classmethod
    def get(cls, task, key):
        if f"{task}#{key}" not in cls.data:
            raise ValueError(f"Artifact '{task}#{key}' not found")
        logger.info(
            f"Artifact Retrieved: {task}#{key} = {cls.data.get(f'{task}#{key}')}"  # noqa
        )
        return cls.data.get(f"{task}#{key}")

    def clear(self):
        for k in self.__dict__.keys():
            if not k.startswith("_"):
                delattr(self, k)

    @classmethod
    def items(cls):
        return cls.data
