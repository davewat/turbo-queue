WIP *Not ready for PRs yet*

### Install
```
pip install turbo-queue
```

### TBD:
- timout for enqueue events to move to avail
- cleanup on restart:
  - move all assigned_ to avail_
  - move all loading_ to avail_


# Turbo Queue

Turbo Queue was designed to solve a specific performance concern with Multiprocessing Queues in Python.  We found that as the number of processes subscribed to a single Multiprocessing Queue increased, the performance on the processes decreased.  This appeared to be due to contention, as the various processes are locking/unlocking the queue to get data.

### Use Case

Our use case involved pulling data from a Kafka topic, running CPU intensive operations to further process the data, and then pushing it back into another Kafka topic.  We were moving billions of events each day.

### Solution
Our solution was to develop the Turbo Queue class.  It is a shared nothing "queue" that uses the file system to provide the basis for coordination between the process.  We use an agreed upon set of rules (as defined in the class) to create and manage files to store and process the data.  SQLite is used a high-performance file storage format.  File names (and re-naming) are used as the control mechanisms.  In our use case we work in batches of data, and that is utilized here as well.  We allow tuning of the batch size (which equates to the rows in the SQLite file) for optimal performance.  A side benefit of the shared nothing approach allows entirely seperate applications (not just sub-processes) to use the queue.

### Results
Before Turbo Queue, using the built-in Multiprocessing Queue resulted in deminishing returns with 3 or more processes.  While performance increased with each process, the throughput was not well correlated to the number of processes.  
  
However, with Turbo Queue, we were able to max out our processors on a single system(40+) with a substancial throughput increase.  We actually began to hit limitations with our network and Kafka stack.  We were able to reduce our requirements from 40 python applications (running 2 subprocesses each), to 2 python apps with 30 subprocesses.

### Will this help you?
YMMV.  This worked well for our use case: CPU intesive processes, moving large volumes of data to and from Kafka, and CPU dense hardware. The on-disk batches also provided insight to troubleshoot other issues further.  However, this may not be helpful in every situation.


# Quickstart:
### Enqueue data
```
## very basic example:

import turbo_queue
enqueue = turbo_queue.enqueue()
enqueue.queue_name = "data_in"
enqueue.root_path = "/path/to/queue"
enqueue.max_avail_files = 5
enqueue.max_events_per_file = 1000
enqueue.start()

enqueue.update_enqueue_active_state()
while enqueue.enqueue_active:
    enqueue.add("""{"data":"add_to_queue"}""")
    enqueue.update_enqueue_active_state()
    # update_enqueue_active_state needs to be run at some frequency
    # if the number of available files exceeds the max, the state will will be set to False
    # this is the mechanism used to address downstream backup in the queue
```

### Dequeue data
```
## very basic example
dequeue = fast_queue.dequeue(1)
dequeue.root_path = "/path/to/queue"
dequeue.queue_name = "data_in"

get_data = dequeue.get()
data = next(get_data)
while data:
    data = next(get_data)
# data = None when the queue is currently empty
# call next(get_data) again to check the queue for more data
```

### How it works

#### Enqueue - loading data into a queue
A process begins using the enqueue class to load data into the "queue".  The "queue" is made up of files on disk.  The files are batches of data.  When the process has **filled** a batch, either by reaching the maximum items for a batch or by timeout (both configurable), the batch is "rolled" to be made "available" to a dequeue process to use, and a new batch file is created and filled.

#### Dequeue - unloading data from a queue
A process beging using the dequeue class to unload data from a "queue".  The "dequeue" process begins by scanning the assigned queue folder for any "available" files, which are "batches" of data.  If one is found, it takes ownership of the file to ensure another process doesn't use it.  Once owned (by a renaming process) the data items are "yielded" to the calling process.  When the batch is exausted, the file is deleted, and the dequeue process begins again to look for any available batches.

#### File naming steps:

1 - Enqueue creates a new, unique file for a batch of data:
```
loading_<epoch_time>_<uuid>.db
```
2 - Enqueue has filled the batch, and rolls it to avail:
```
#rename:
loading_<epoch_time>_<uuid>.db
# to:
avail_<epoch_time>_<uuid>.db
3 - Enqueue restarts the process, and creates a new, unique file for a batch of data

4 - Dequeue searches for files named avail_*.  When one is selected, it is renamed to be used exclusively by the process:
```
# rename:
avail_<epoch_time>_<uuid>.db
# to:
assigned_{self.proc_num}_<epoch_time>_<uuid>.db
```
The proc_num is provided by the calling process.  It allows insight to the process that is using the actual file.  Mostly useful for troubleshooting.

5 - When dequeue has yielded all of the data in the batch, the file is deleted, and Dequeue looks for the next available avail_* file

