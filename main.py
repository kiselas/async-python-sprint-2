import os
import time
from copy import copy
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

import requests

from exceptions import TaskError
from job import Job, check_dependencies
from logger import logger
from scheduler import Scheduler


class TestTask(Job):
    def __init__(self, start_at=None, max_working_time=-1, max_tries=0, dependencies=None):
        super().__init__(start_at, max_working_time, max_tries, dependencies)
        self.first_stage = None
        self.second_stage = None
        self.third_stage = None

    @check_dependencies
    def run(self):
        try:
            if not self.first_stage:
                print("TestTask first_stage 1")
                yield self
                print("TestTask first_stage 2 ")
                yield self
                print("TestTask first_stage 3")
                yield self
                self.first_stage = "first_stage"
            if not self.second_stage:
                yield self
                print("TestTask second_stage 4")
                yield self
                print("TestTask second_stage 5")
                yield self
                self.second_stage = "second_stage"
            if not self.third_stage:
                print("TestTask third_stage 6")
                yield self
                print("TestTask third_stage 7")
                yield self
                self.third_stage = "third_stage"
        except TypeError:
            logger.info("Ошибка TypeError, нужно заретраить")
            yield self, TaskError

    def reset(self):
        """Необходимо реализовать сброс состояния для ретраев"""
        logger.info("Сбрасываем данные об этапах")
        self.first_stage = None
        self.second_stage = None
        self.third_stage = None


class CreateNewDirsTask(Job):
    def __init__(self, new_dirs: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.new_dirs = copy(new_dirs)
        self.new_dirs_dump = copy(new_dirs)

    def run(self):
        if self.new_dirs:
            new_dir = self.new_dirs.pop()
            os.makedirs(new_dir, exist_ok=True)
            logger.info(f"Создана папка: {new_dir}")
            yield self

    def reset(self):
        self.new_dirs = copy(self.new_dirs_dump)


class CreateNewFilesTask(Job):
    def __init__(self, dirs_to_create_file: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dirs_to_create_file = copy(dirs_to_create_file)
        self.dirs_to_create_file_dump = copy(dirs_to_create_file)

    def run(self):
        if self.dirs_to_create_file:
            file_path = f"{self.dirs_to_create_file.pop()}/testfile.txt"
            with open(file_path, "w"):
                pass  # Создаем пустой файл
            logger.info(f"Создан файл: {file_path}")
            yield self

    def reset(self):
        self.dirs_to_create_file = copy(self.dirs_to_create_file_dump)


class SaveWebPagesTask(Job):
    def __init__(self, urls: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls = copy(urls)
        self.urls_dump = copy(urls)

    def run(self):
        for _ in range(len(self.urls)):
            yield self
            url = self.urls.pop()
            response = requests.get(url)
            parsed_url = urlparse(url)
            hostname = parsed_url.netloc
            timestamp = time.time()
            filename = f"{hostname}_{timestamp}.txt"
            with open(filename, "wb") as file:
                file.write(response.content)
            logger.info(f"Создан файл: {filename}")
            yield self

    def reset(self):
        self.urls = copy(self.urls_dump)


current_time = datetime.now()
start_time = current_time + timedelta(minutes=1)

new_dirs = [
    "./test1/",
    "./test2/",
    "./test3/",
    "./test4/",
    "./test5/",
]
urls = [
    "https://music.yandex.ru/home",
    "https://habr.com/ru/feed/",
    "https://dzen.ru/",
    "https://ya.ru",
]

test_task = TestTask(max_tries=3)
create_dirs_task = CreateNewDirsTask(max_tries=3, new_dirs=new_dirs)
save_files_task = CreateNewFilesTask(max_tries=3, dirs_to_create_file=new_dirs, dependencies=[create_dirs_task])
save_web_pages_task = SaveWebPagesTask(max_tries=3, dependencies=[save_files_task], urls=urls)

scheduler = Scheduler()
scheduler.schedule(create_dirs_task)
scheduler.schedule(save_files_task)
scheduler.schedule(save_web_pages_task)
scheduler.run()
