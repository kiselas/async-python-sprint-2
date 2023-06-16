import os
import pickle
from typing import List

from logger import logger
from settings import DONE_TASKS


def get_pickled_tasks(from_dir):
    file_paths = [f"{from_dir}{file}" for file in os.listdir(from_dir) if file.endswith(".pkl")]
    objects = []
    for file_path in file_paths:
        with open(file_path, "rb") as file:
            obj = pickle.load(file)
            objects.append(iter(obj))
    logger.info(f"Restore {len(objects)} objects from dir {from_dir}")
    return objects


def delete_files_in_dir(folder_path):
    file_list = os.listdir(folder_path)

    # Удаляем каждый файл в папке
    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
    logger.info(f"Successfully delete all files in {folder_path}")


def check_task_in_completed(unique_name):
    with open(DONE_TASKS) as file:
        return any(line.strip() == unique_name for line in file)


def check_tasks_is_completed(dependencies: List):
    if not dependencies:
        return True
    task_statuses = []
    for dependency in dependencies:
        with open(DONE_TASKS) as file:
            task_statuses.append(any(line.strip() == dependency.unique_name for line in file))
    return all(task_statuses)
