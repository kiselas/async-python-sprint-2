import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from exceptions import TaskError
from job import Job
from logger import logger
from scheduler import Scheduler
from utils import check_task_in_completed


class TestJob(Job):
    def __init__(self, start_at=None, max_working_time=-1, max_tries=0, dependencies=None):
        super().__init__(start_at, max_working_time, max_tries, dependencies)
        self.first_stage = None
        self.second_stage = None
        self.third_stage = None

    def run(self):
        try:
            if not self.first_stage:
                print("TestTask first_stage 1")
                yield self, None
                print("TestTask first_stage 2 ")
                yield self, None
                print("TestTask first_stage 3")
                yield self, None
                self.test_method()
                self.first_stage = "first_stage"
            if not self.second_stage:
                yield self, None
                print("TestTask second_stage 5")
                yield self, None
                print("TestTask second_stage 6")
                yield self, None
                self.second_stage = "second_stage 4"
        except TypeError:
            logger.info("Ошибка TypeError, нужно заретраить")
            yield self, TaskError

    def test_method(self):
        pass

    def reset(self):
        """Необходимо реализовать сброс состояния для ретраев"""
        logger.info("Сбрасываем данные об этапах")
        self.first_stage = None
        self.second_stage = None


class SchedulerTestCase(unittest.TestCase):
    def setUp(self):
        self.scheduler = Scheduler()

    def test_schedule_and_run_single_task(self):
        job = TestJob()
        self.scheduler.schedule(job)
        self.scheduler.run()

        # Проверяем, что задача выполнена
        self.assertTrue(check_task_in_completed(job.unique_name))

    def test_schedule_and_run_task_with_dependencies(self):
        dependency_job = TestJob()
        dependent_job = TestJob(dependencies=[dependency_job])

        # Добавляем задачи в планировщик
        self.scheduler.schedule(dependent_job)
        self.scheduler.schedule(dependency_job)

        # Запускаем планировщик
        self.scheduler.run()

        # Проверяем, что задачи выполнены
        self.assertTrue(check_task_in_completed(dependency_job.unique_name))
        self.assertTrue(check_task_in_completed(dependent_job.unique_name))

    def test_schedule_and_run_delayed_task(self):
        start_at = datetime.now() + timedelta(seconds=10)
        job = TestJob(start_at=start_at)

        # Добавляем задачу в планировщик
        self.scheduler.schedule(job)

        # Запускаем планировщик
        self.scheduler.run()

        # Проверяем, что задача выполнена после заданного времени
        self.assertTrue(check_task_in_completed(job.unique_name))

    def test_schedule_and_run_long_running_task(self):
        max_working_time = timedelta(seconds=10)
        job = TestJob(max_working_time=max_working_time)

        # Добавляем задачу в планировщик
        self.scheduler.schedule(job)

        # Запускаем планировщик
        self.scheduler.run()

        # Проверяем, что задача завершена после превышения времени выполнения
        self.assertTrue(check_task_in_completed(job.unique_name))

    def test_schedule_and_run_task_with_max_retries(self):
        max_tries = 3
        job = TestJob(max_tries=max_tries)

        # Мокаем выполнение задачи с ошибкой
        job.test_method = MagicMock(side_effect=TaskError)

        # Добавляем задачу в планировщик
        self.scheduler.schedule(job)

        # Запускаем планировщик
        self.scheduler.run()

        # Проверяем, что задача была перезапущена максимальное количество раз
        self.assertEqual(job.tries, max_tries)

    def test_scheduler_resumes_execution_after_stop(self):
        job1 = TestJob()
        job2 = TestJob()

        # Добавляем задачи в планировщик
        self.scheduler.schedule(job1)
        self.scheduler.schedule(job2)

        # Запускаем планировщик и останавливаем его
        self.scheduler.run()
        self.scheduler.stop()

        # Повторно запускаем планировщик
        self.scheduler.run()

        # Проверяем, что задачи продолжили выполнение
        self.assertTrue(check_task_in_completed(job1.unique_name))
        self.assertTrue(check_task_in_completed(job2.unique_name))


if __name__ == "__main__":
    unittest.main()
