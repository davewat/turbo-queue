__all__ = ["dequeue", "enqueue", "startup"]

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from os import remove as file_remove
from os import rename
from pathlib import Path


class _turbo_queue_base:
    def __init__(self):
        # self._age = 0
        self._queue_name = "example_queue_name"  # name of the queue
        self._root_path = "/dc_data"
        # self._queue_folder = ''
        self._num_loaders = (
            1  # number of processes that will be loading the files (inbound)
        )
        self._num_queues = 1  # number of queues/processes that will have the loaded files distributed amongst
        self._max_ready_files = (
            1  # maximum number of ready files - loading will pause when reached
        )
        self._max_events_per_file = 80000  # maximum number of events per DB file that will be loaded, before a new DB file is created
        self._max_batch_age = 10  # approximate maximum number of seconds before a batch is flushed and a new one created
        self._enqueue_active = False  # pause/ resume loading to file
        self._dequeue_active = True  # pause/ resume loading to queues
        self._processing_hold = (
            False  # additional hold while processing DB rename and recreate
        )
        self._remove_invalid = True  # remove files marked as invalid

        # auto-recover stale events in the queue folder
        # existing loading will be renamed to ready, to be picked up by the dequeue process when it is started
        # existing or stale chosen
        self._recover_on_restart = (
            True  # on start, in-case of shutdown while processing
        )
        self._recover_on_stale = (
            True  # while running, check for stale events if processing stops
        )
        self._recover_on_stale_check_frequency = 300  # frequency to check in seconds
        self._loading_stale_age = (
            self._max_batch_age * 2
        )  # default is _max_batch_age * 2 (seconds)
        self._chosen_stale_age = (
            100  # this should be adjusted based on average time it takes to rpocess
        )

        self._max_chosen_age = 100

        self._drop_overflow = False  # default of FALSE - used for TESTING ONLY - will DROP overflow rather than just change fold status
        self._db_conn = None
        self._db_cursor = None
        self._db_name = None
        self._db_path = None

    # utility:
    def get_current_epoch_int(self):
        return int(time.time())

    def get_uuid(self):
        return uuid.uuid4()

    #
    # queue_name - name of the queue
    @property
    def queue_name(self):
        return self._queue_name

    @queue_name.setter
    def queue_name(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be a string")
        self._queue_name = a

    #
    # root_path - path from root to folder holding the queue
    @property
    def root_path(self):
        return self._root_path

    @root_path.setter
    def root_path(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be a string")
        self._root_path = a

    """
    #
    # queue_folder - path from root to folder holding the queue
    @property
    def queue_folder(self):
        return self._queue_folder
    @queue_folder.setter
    def queue_folder(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be a string")
        self._queue_folder = a
    """

    #
    # num_loaders - number of processes that will be loading the files (inbound)
    @property
    def num_loaders(self):
        return self._num_loaders

    @num_loaders.setter
    def num_loaders(self, a):
        if type(a) != int or a < 1:
            raise ValueError("Sorry, num_loaders must be an integer > 0")
        self._num_loaders = a

    #
    # num_queues - number of queues/processes that will have the loaded files distributed amongst
    @property
    def num_queues(self):
        return self._num_queues

    @num_queues.setter
    def num_queues(self, a):
        if type(a) != int or a < 1:
            raise ValueError("Sorry, num_queues must be an integer > 0")
        self._num_queues = a

    #
    # max_ready_files - maximum number of ready(able) files - loading will pause when reached
    @property
    def max_ready_files(self):
        return self._max_ready_files

    @max_ready_files.setter
    def max_ready_files(self, a):
        if type(a) != int or a < 1:
            raise ValueError("Sorry, max_ready_files must be an integer > 0")
        self._max_ready_files = a

    #
    # max_events_per_file - maximum number of events in a batch - a new batch will be generated and used when reached, and this will be made ready
    @property
    def max_events_per_file(self):
        return self._max_events_per_file

    @max_events_per_file.setter
    def max_events_per_file(self, a):
        if type(a) != int or a < 1:
            raise ValueError("Sorry, max_events_per_file must be an integer > 0")
        self._max_events_per_file = a

    #
    # max_batch_age - number of seconds to check files and update
    @property
    def max_batch_age(self):
        return self._max_batch_age

    @max_batch_age.setter
    def max_batch_age(self, a):
        if type(a) != int or a < 1:
            raise ValueError("Sorry, max_batch_age must be an integer > 0")
        self._max_batch_age = a

    #
    # enqueue_active - pause/ resume loading to file
    @property
    def enqueue_active(self):
        return self._enqueue_active

    @enqueue_active.setter
    def enqueue_active(self, a):
        if type(a) != bool:
            raise ValueError("Sorry, enqueue_active must be True or False")
        self._enqueue_active = a

    #
    # dequeue_active - pause/ resume loading to queues
    @property
    def dequeue_active(self):
        return self._dequeue_active

    @dequeue_active.setter
    def dequeue_active(self, a):
        if type(a) != bool:
            raise ValueError("Sorry, dequeue_active must be True or False")
        self._dequeue_active = a

    #
    # processing_hold - pause/ resume loading to queues
    @property
    def processing_hold(self):
        return self._processing_hold

    @processing_hold.setter
    def processing_hold(self, a):
        if type(a) != bool:
            raise ValueError("Sorry, processing_hold must be True or False")
        self._processing_hold = a

    #

    # drop_overflow - pause/ resume loading to queues
    @property
    def drop_overflow(self):
        return self._drop_overflow

    @drop_overflow.setter
    def drop_overflow(self, a):
        if type(a) != bool:
            raise ValueError("Sorry, drop_overflow must be True or False")
        self._drop_overflow = a

    #
    # remove_invalid - remove files marked as invalid
    @property
    def remove_invalid(self):
        return self._remove_invalid

    @remove_invalid.setter
    def remove_invalid(self, a):
        if type(a) != bool:
            raise ValueError("Sorry, remove_invalid must be True or False")
        self._remove_invalid = a

    #
    def setup_logging(
        self, logger_name=__name__, level=logging.WARNING, log_file="turbo_queue.log"
    ):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level)
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            "%(asctime)s : %(levelname)s : %(name)s : %(message)s"
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _close_db(self):
        self._db_conn.commit()
        self._db_cursor.close()
        self._db_conn.close()
        self._db_conn = None
        return

    def _recover_stale(self, match_prefix, new_prefix):
        print(f"recover_stale: {match_prefix} {new_prefix}")
        path_to_files = f"{self.root_path}/{self.queue_name}"
        try:
            matched_files = sorted(Path(path_to_files).glob(match_prefix))
        except:
            matched_files = []
        if len(matched_files) > 0:
            for file in matched_files:
                new_file_path = f"{self.root_path}/{self.queue_name}"
                file_error = False
                # validate SQLITE integrity:
                con = sqlite3.connect(file)
                cur = con.cursor()
                try:
                    cur.execute("PRAGMA integrity_check")
                except sqlite3.DatabaseError:
                    file_error = True
                con.close()
                if not file_error:
                    try:
                        if os.path.getsize(file) == 0:
                            file_error = True
                    except:
                        file_error = True
                if file_error:
                    if self._remove_invalid:
                        try:
                            os.remove(file)
                        except:
                            rename(
                                file,
                                f"{new_file_path}/invalid_file_{self.get_current_epoch_int()}_{self.get_uuid()}_recovered.db",
                            )
                    else:
                        rename(
                            file,
                            f"{new_file_path}/invalid_file_{self.get_current_epoch_int()}_{self.get_uuid()}_recovered.db",
                        )
                else:
                    rename(
                        file,
                        f"{new_file_path}/{new_prefix}_{self.get_current_epoch_int()}_{self.get_uuid()}_recovered.db",
                    )
        print(f"recover_stale complete: {match_prefix} {new_prefix}")
        return

    def get_ready_length(self):
        """
        get count of *.ready files
        """
        path_to_ready = f"{self.root_path}/{self.queue_name}"
        try:
            matched_files = sorted(Path(path_to_ready).glob("ready_*"))
        except:
            matched_files = []
        return len(matched_files)

    def update_enqueue_active_state(self):
        """
        check the number of ready(able) files and update the state if it does/not exceed the max
        """
        if self.enqueue_active == False and self.processing_hold == False:
            if self.get_ready_length() < self.max_ready_files:
                self.enqueue_active = True
                self.logger.info(
                    f"update_enqueue_active_state to True {self._queue_name}"
                )
        elif self.get_ready_length() >= self.max_ready_files:
            self.enqueue_active = False
        return

    def check_path_and_create(self, db_path):
        isExist = os.path.exists(db_path)
        if not isExist:
            # Create a new directory because it does not exist
            try:
                os.makedirs(db_path)
            except FileExistsError:
                # the folder was already create by another system process connecting
                pass
            except Exception as e:
                print(f"create error {e}")
        return

    def get_next_ready_file(self):
        """
        get the next ready file
        return None if none ready or error
        """
        pp = f"{self.root_path}/{self.queue_name}"
        try:
            next_file = str(sorted(Path(pp).glob("ready_*"))[0])
        except:
            next_file = None
        return next_file


class startup(_turbo_queue_base):
    """class with methods for startup operations"""

    def __init__(self):
        self.desc = "turbo_queue class for queue high performance"
        super().__init__()

    def on_start_cleanup(self):
        """
        call once prior to starting queues
        recovers any stale batches that were left uprocessed, by moving them to ready, to be picked up by the queue on restart.
        """
        self._recover_stale("loading_*", "ready")
        self._recover_stale("chosen_*", "ready")
        #
        # for upgrade:
        self._recover_stale("avail_*", "ready")
        self._recover_stale("assigned_*", "ready")
        return


class enqueue(_turbo_queue_base):
    """class with methods for loading the queue"""

    def __init__(self):
        self.desc = "turbo_queue class for queue high performance"
        super().__init__()
        self._create_epoch = 1  # default value to start
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        # self.loop.create_task(self.check_batch_age())
        # self.loop.run_forever()

    #
    def _new_loading_db(self):
        path = f"{self.root_path}/{self.queue_name}"
        # Check whether the specified path exists or not
        self.check_path_and_create(path)
        self._create_epoch = self.get_current_epoch_int()
        self._db_name = f"{self._create_epoch}_{self.get_uuid()}.db"
        self.db_state = "loading"
        self._db_path = (
            f"{self.root_path}/{self.queue_name}/{self.db_state}_{self._db_name}"
        )
        # improve:
        # try:
        # creating database for first time{self._db_path}
        self._db_conn = sqlite3.connect(self._db_path)
        self._db_conn.execute("pragma synchronous=0;")
        # except:
        #    pass
        self._db_conn.execute("pragma synchronous=0;")
        self._db_cursor = self._db_conn.cursor()
        self._db_cursor.execute("CREATE TABLE IF NOT EXISTS logs (log_data json)")
        self._db_conn.commit()
        # test transaction:
        self._db_cursor.execute("BEGIN TRANSACTION")

        self.row_count = 0
        return

    #
    def insert_loading_db(self, log_data):
        """
        insert data into DB
        """
        self._db_cursor.execute(
            """INSERT INTO logs (log_data) VALUES (?)""", (json.dumps(log_data),)
        )
        return

    #
    def start(self):
        """
        create an initial DB to begin loading data into
        """
        self._new_loading_db()
        return

    #
    def add(self, dict_to_add):
        """
        add data to queue
        """
        if self.enqueue_active == False and self._drop_overflow == True:
            # dont insert, we are dropping overflow
            # WIP
            pass
        else:
            self.insert_loading_db(dict_to_add)
            self.row_count += 1
            if self.row_count >= self.max_events_per_file:
                self._roll_next_batch()
        return

    def _roll_next_batch(self):
        # put on hold while processing, and force re-check on next loop
        self.processing_hold = True
        self.enqueue_active = False
        #
        self.row_count = 0
        self._close_db()
        rename(
            self._db_path, f"{self.root_path}/{self.queue_name}/ready_{self._db_name}"
        )
        self._new_loading_db()
        self.update_enqueue_active_state()
        self.processing_hold = False
        return

    def check_time(self):
        """
        provides method to check the time the batch has been loading, and if exceeds timeout,
        roll to the next batch
        """

        return

    async def check_batch_age(self):
        await asyncio.sleep(self._max_batch_age)
        if (
            self._create_epoch + int(self._max_batch_age)
        ) < self.get_current_epoch_int():
            self._roll_next_batch()
        self.loop.create_task(self.check_batch_age())


class dequeue(_turbo_queue_base):
    """Class to remove data from a Turbo Queue

    Parameters
    ----------
    proc_num(required)
        An integer to uniquely identify this process vs other processes that will dequeue from this queue.

    Returns
    -------
    New dequeue object
        A dequeue object.  Additional parameters can be set.  Primary function is get()

    Examples
    --------
    >>> import turbo_queue
    >>> my_out_queue = turbo_queue.dequeue(1)
    >>> my_out_queue.root_path = '/path/to/queue'
    >>> my_out_queue.queue_name = 'my_queue'
    >>> get_data = my_out_queue.get()
    >>> doc = next(get_data)
    >>> while doc:
            #<do something with doc>
    """

    def __init__(self, proc_num=None):
        self.desc = "turbo_queue class for high performance IPC"
        super().__init__()
        self.proc_num = None
        self._total_gets = 0
        self._total_batch = 0
        self._auto_cleanup = True

    #
    # auto_cleanup
    @property
    def auto_cleanup(self):
        return self._auto_cleanup

    @auto_cleanup.setter
    def auto_cleanup(self, a):
        if type(a) != bool:
            raise ValueError("Sorry, auto_cleanup must be True or False")
        self._auto_cleanup = a

    #
    #
    def check_state(self):
        """
        future use - method to check for error state
        """
        pass

    def clean_up(self):
        """
        clean-up process called to remove the current batch after all events have been processed
        this is automatically called by default.
        Setting auto_cleanup to False will allow you to call in manually (or raise errors and not process).
        You MUST call cleanup() after the last get()-yield if you wish to continue to the next ready batch,
        otherwise, you are choosing to leave the batch
        """
        self._close_db()
        file_remove(self._db_path)
        self._db_name = None
        self._db_path = None
        return

    #
    def get(self):
        """Get data from Turbo Queue

        Parameters
        ----------
        none

        Returns
        -------
        A yieldable pointer to the next data set
        OR
        None, when batch is completed (no more data) or there is no ready batch

        Examples
        --------
        >>> get_data = my_out_queue.get()
        >>> doc = next(get_data)
        >>> while doc:
                <do something with doc>
        """
        if self._db_path == None:
            result = self._get_chosen_from_ready()
            if result == False:
                yield None
            else:
                self._total_batch += 1
        for row in self._db_conn.execute("SELECT log_data FROM logs"):
            # WIP
            # self.totalMessages += 1
            # pollCount += 1
            # rowcount += 1
            data = json.loads(row[0])
            self._total_gets += 1
            yield data
        if self._auto_cleanup:
            self.clean_up()
        yield None

    #
    def total_gets(self):
        """
        return count of total gets returned
        """
        return self._total_gets

    #
    def total_batch(self):
        """
        return count of total batches opened
        """
        return self._total_batch

    def _open_db(self):
        try:
            # opening database for first time{self._db_path}
            self._db_conn = sqlite3.connect(self._db_path)
            self._db_conn.execute("pragma synchronous=0;")
            self._db_cursor = self._db_conn.cursor()
            return True
        except:
            self.logger.info(f"ERROR opening database for first time{self._db_path}")
            return False

    def _get_chosen_from_ready(self):
        """
        get a path to a 'chosen' file to process, from one of the 'ready'able files
        will attempt max_attempts times to get a ready file set to self_db_path
        will return True when successful or when already chosen
        will return False after 10 failed attempts, allowing the calling process to reset and try again
        """
        attempts = 0
        max_attempts = 10
        while attempts < max_attempts:
            attempts += 1
            path1 = f"{self.root_path}/{self.queue_name}"
            self.check_path_and_create(path1)
            next_file = self.get_next_ready_file()
            if next_file:
                try:
                    # quickly grab the file
                    rename(next_file, f"{next_file}_temp_hold")
                    filename = os.path.basename(next_file)
                    filename_parts = filename.split("_", 1)
                    if self.proc_num:
                        new_name = f"chosen_{filename_parts[1]}_{self.proc_num}"
                    else:
                        new_name = f"chosen_{filename_parts[1]}"
                    new_path = f"{self.root_path}/{self.queue_name}/{new_name}"
                    rename(f"{next_file}_temp_hold", new_path)
                    self._db_path = new_path
                    self._db_name = new_name
                    result_open = self._open_db()
                    if result_open == False:
                        self._db_path = None
                    return result_open
                except:
                    # file was probably moved by someone else:
                    pass
            # wait 0.05 second, and try again
            time.sleep(0.05)
        return False
