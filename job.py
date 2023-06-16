import pickle
import uuid
from datetime import datetime

from logger import logger
from settings import DONE_TASKS, QUEUED_TASKS_DIR, RUNNING_TASKS_DIR


class Job:
    def __init__(self, start_at=None, max_working_time=-1, max_tries=0, dependencies=None):
        self.start_at = start_at or datetime.now()
        self.max_working_time = max_working_time
        self.max_tries = max_tries
        self.tries = 0
        self.dependencies = dependencies or []
        self.unique_name = str(uuid.uuid4())

    def __iter__(self):
        yield from self.run()

    def run(self):
        """Для реализации сохранения состояний, необходимо поделить код на этапы выполнения, которые будут
        определены как None в __init__"""
        raise NotImplementedError

    def reset(self):
        """Необходимо реализовать сброс состояния для ретраев"""
        raise NotImplementedError

    def stop(self, save_data, running) -> None:
        """Штатная остановка с сохранением всех нужных данных"""
        if not save_data:
            logger.info(f"Exit without saving {self.unique_name}")
            return
        if running:
            path_to_save = f"{RUNNING_TASKS_DIR}{self.unique_name}.pkl"
        else:
            path_to_save = f"{QUEUED_TASKS_DIR}{self.unique_name}.pkl"
        logger.info(f"Saving data for task  {self.unique_name}")
        with open(path_to_save, "wb") as file:
            pickle.dump(self, file)
        logger.info(f"Successfully save data for task {self.unique_name}")

    @property
    def is_expired(self) -> bool:
        """Проверяет истекло ли время выполнения задачи"""
        if self.max_working_time != -1:
            now = datetime.now()
            working_time = now - self.start_at
            if working_time > self.max_working_time:
                return True
        return False

    def save_to_done(self) -> None:
        """Сохраняет идентификатор задачи в текстовый файл с выполненными задачами"""
        logger.info("Add to done tasks")
        with open(DONE_TASKS, "a") as file:
            file.write(self.unique_name + "\n")
