__all__ = ["MultiTask", "TaskGroup"]
import asyncio
import atexit
import logging
import multiprocessing as mp
import os

logger = mp.log_to_stderr(logging.INFO)

import signal
import time
from sys import exit

logging.basicConfig(filename="./test_multi.log", level=logging.INFO)
# logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")
logging.Formatter(
    "%(asctime)s,%(msecs)d %(levelname)-8s [%(pathname)s:%(lineno)d in function %(funcName)s] %(message)s",
    "%Y-%m-%d:%H:%M:%S",
)
logging.propagate = True


class TaskGroup:
    def __init__(self):
        """
        build a task to run
        will automatically add a 'process_id' as the last kwargs to pass to your task
        """
        self.function_to_call = None  # pass the actuall function to call
        self.kwargs = {}  # kwargs as dict {'arg1':123,'arg2':456}
        self.daemon = True  # run as daemon
        self.include_task_number = (
            True  # will pass the process counter as kwarg task_number
        )
        self.process_count = 1  # number of independent processes to start
        self.process_number_start = (
            11  # start labeling the task number, beginning with this value +1
        )


class MultiTask:
    def __init__(self):
        """Class to help in spinning up many processes for an application"""
        self.process_list = []
        self.loop = asyncio.new_event_loop()
        self.task_group_list = []
        logging.info("class loaded")

    def _goodbye(self, name, adjective, *args):
        logging.info(f"Shutting down processes")
        for my_proc in self.process_list:
            logging.info(f"PROC: {str(my_proc.name)}")
            my_proc.terminate()
            my_proc.join()
            logging.info(f"PID: {str(os.getpid())}")
        logging.info("Goodbye %s, it was %s to meet you." % (name, adjective))

    def _handler(self, signal_received, frame):
        logging.info("SIGINT or CTRL-C detected. Exiting gracefully")
        exit(0)

    def add_task(self, task):
        if task.function_to_call:
            self.task_group_list.append(task)

    def start_all(self):
        atexit.register(self._goodbye, adjective="nice", name="User")
        logging.info("Loading _main_")
        logging.debug("main_debug...")
        logging.info("main_info...")
        signal.signal(signal.SIGTERM, self._handler)  # kill pid
        signal.signal(signal.SIGINT, self._handler)  # ctlr + c
        signal.signal(signal.SIGTSTP, self._handler)  # ctlr + z

        for process_group in self.task_group_list:
            # logging.info(process_group)
            logging.info(f"Process Group: {process_group.kwargs}")
            pg_start = process_group.process_number_start
            pg_end = process_group.process_number_start + process_group.process_count
            logging.info(f"Process Group Range: {pg_start} :: {pg_end}")
            for process_id in range(pg_start, pg_end):
                process_group.kwargs["process_id"] = process_id
                logging.info(f"Process Group ID {process_group.kwargs['process_id']}")
                my_proc = mp.Process(
                    target=process_group.function_to_call, kwargs=process_group.kwargs
                )
                my_proc.daemon = True
                my_proc.start()
                self.process_list.append(my_proc)
        logging.info(f"process_list: {self.process_list}")
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
