import json
import os
import time
from datetime import datetime
from typing import Dict, Iterator, List

from exceptions import StopEventLoop, TaskError
from job import Job
from logger import logger
from settings import CONDITION_CACHE, DONE_TASKS, POOL_SIZE, QUEUED_TASKS_DIR, RUNNING_TASKS_DIR, SCHEDULER_DATA
from utils import check_tasks_is_completed, delete_files_in_dir, get_pickled_tasks


class Scheduler:
    def __init__(self):
        self.init_dirs()
        self.tasks: List[Job] = []
        self.running_tasks: List[Iterator] = []
        self.tasks_mapping: Dict[Iterator, Job] = {}
        if self.is_resume_after_stop():
            self.init_from_file()
        else:
            # перезаписать файл с выполненными задачами
            self.create_done_list()

    def init_from_file(self):
        logger.info("Start init from file}")
        self.tasks = get_pickled_tasks(from_dir=QUEUED_TASKS_DIR)
        self.running_tasks = get_pickled_tasks(from_dir=RUNNING_TASKS_DIR)
        logger.info("Init successfull")
        logger.debug(f"tasks: {self.tasks}")
        logger.debug(f"running_tasks: {self.running_tasks}")

    @staticmethod
    def init_dirs():
        try:
            os.makedirs(QUEUED_TASKS_DIR, exist_ok=True)
            os.makedirs(RUNNING_TASKS_DIR, exist_ok=True)
            logger.info("All needed dirs exists")
        except PermissionError:
            logger.error("No permissions to create directories", exc_info=True)

    def schedule(self, task):
        self.tasks.append(task)

    def run(self): # noqa C901
        try:
            while any([self.tasks, self.running_tasks]):
                for _ in range(min(POOL_SIZE, len(self.tasks))):
                    task: Job = self.tasks.pop(0)
                    if task.start_at and task.start_at > datetime.now():
                        logger.info(f"The scheduled execution time of the {task} task has not yet arrived, waiting")
                        self.tasks.append(task)
                        continue

                    if not check_tasks_is_completed(task.dependencies):
                        logger.info(f"Not all {task} dependencies are met, waiting")
                        self.tasks.append(task)
                        continue

                    task_iterator = iter(task)
                    self.tasks_mapping[task_iterator] = task
                    self.running_tasks.append(task_iterator)

                for running_task in self.running_tasks:
                    if not self.is_running():
                        # проверяем запущен ли луп
                        raise StopEventLoop
                    try:
                        next(running_task)

                        if self.tasks_mapping[running_task].is_expired:
                            raise TimeoutError

                    except StopIteration:
                        logger.debug("Задача выполнена")
                        self.running_tasks.remove(running_task)
                        self.tasks_mapping[running_task].save_to_done()
                        del self.tasks_mapping[running_task]
                    except TaskError:
                        task_instance = self.tasks_mapping[running_task]
                        if task_instance.max_tries > task_instance.tries:
                            task_instance.tries += 1
                            logger.debug(f"max_tries - {task_instance.max_tries}, tries - {task_instance.tries}")
                            # сбрасываем сохраненные этапы
                            task_instance.reset()
                            # удаляем старый итератор
                            self.running_tasks.remove(running_task)
                            del self.tasks_mapping[running_task]
                            # добавляем новый для новой попытки
                            task_iterator = iter(task_instance.run())
                            self.running_tasks.append(task_iterator)
                            self.tasks_mapping[task_iterator] = task_instance
                        else:
                            logger.warning("Был достигнут максимум повторов выполнения задачи")
                            self.running_tasks.remove(running_task)
                            del self.tasks_mapping[running_task]
                    except TimeoutError:
                        logger.warning("Был достигнут максимум времени на выполнение задачи")
                        self.running_tasks.remove(running_task)
                        del self.tasks_mapping[running_task]
                    except Exception:
                        logger.error("Непредвиденная ошибка, дальнейшее выполнение задачи невозможно", exc_info=True)
                        self.running_tasks.remove(running_task)
                        del self.tasks_mapping[running_task]

                time.sleep(1)  # для более наглядной проверки

            logger.info("All tasks done. Start clean up")
            self.clean_up()
        except StopEventLoop:
            logger.info("Get signal to stop from file")
            self.stop()
        except KeyboardInterrupt:
            logger.info("Get stop signal from KeyboardInterrupt")
            self.stop()

    def restart(self):
        self.stop()
        self.run()

    @staticmethod
    def clean_up() -> None:
        clean_dirs = [QUEUED_TASKS_DIR, RUNNING_TASKS_DIR]
        for clean_dir in clean_dirs:
            delete_files_in_dir(clean_dir)

        if os.path.isfile(CONDITION_CACHE):
            os.remove(CONDITION_CACHE)

        if os.path.isfile(SCHEDULER_DATA):
            os.remove(SCHEDULER_DATA)

    def create_done_list(self):
        logger.info("Created file for done tasks")
        with open(DONE_TASKS, "w"):
            pass


    def stop(self, save_data=True) -> None:
        for task in self.tasks:
            task.stop(save_data=save_data, running=False)
        logger.info("List of tasks saved")

        for running_task in self.running_tasks:
            self.tasks_mapping[running_task].stop(save_data=save_data, running=True)
        logger.info("List of running tasks saved")

        with open(SCHEDULER_DATA, "w") as file:
            scheduler_data = {
                "save_data": True,
                "len_queued_tasks": len(self.tasks),
                "len_running_tasks": len(self.running_tasks),
            }
            json.dump(scheduler_data, file)

    @staticmethod
    def is_resume_after_stop() -> bool:
        if os.path.isfile(SCHEDULER_DATA):
            print(f"Файл {SCHEDULER_DATA} существует.")
            return True
        print(f"Файл {SCHEDULER_DATA} не существует.")
        return False

    @staticmethod
    def is_running() -> bool:
        if os.path.isfile(CONDITION_CACHE):
            logger.debug(f"Файл {CONDITION_CACHE} существует.")
            with open(CONDITION_CACHE) as file:
                condition_cache = json.load(file)
                return condition_cache["is_running"]

        with open(CONDITION_CACHE, "w") as file:
            condition = {"is_running": True}
            json.dump(condition, file)
            return True
