

### Install
```
pip install turbo-queue
```
#### Project Status
WIP - Not ready for PRs yet

# Turbo Queue
High performance, multi-processing queue. Works across child processes, and even between individual Python applications!

# Contents
 - [Overview](#overview)
 - [Use Cases](#use-cases)
 - [Solution](#solution)
 - [Results](#results)
 - [Quickstart](#quickstart)
 - [How It Works](docs/how_it_works.md)

## Overview
Turbo Queue is used in place of the Python Multiprocessing Queue with simple add/get semantics, allowing you to create multiple processes that share a queue.

Turbo Queue was designed to improve performance in situations where the Python Multiprocessing Queues is typically used. We found that as the number of processes subscribed to a single Multiprocessing Queue increased, the performance improvement on each successive process decreased. This appeared to be due to contention, with the processes locking/unlocking the queue to load/get data.

## Use Cases
- Stream data from Kafka, run CPU intensive operations on the data, and then stream back to Kafka, on the order of billions of events per day.
- Stream data from Kafka, reformat, and send to Elasticsearch.
- Stream data from Kafka, reformat, and send to a service API.
- Pull data from an API at high volume, process, and send to another API.

## Solution
Our solution was to develop the Turbo Queue class. It is a shared nothing "queue" that uses the file system to provide the basis for coordination between the process. We use an agreed upon set of rules (as defined in the class) to create and manage files to store and process the data. SQLite is used a high-performance file storage format. File names (and re-naming) are used as the control mechanisms. In our use case we work in batches of data, and that is utilized here as well. We allow tuning of the batch size (which equates to the rows in the SQLite file) for optimal performance. A side benefit of the shared nothing approach allows entirely separate applications (not just sub-processes) to use the queue.

## Results
Before Turbo Queue, using the built-in Multiprocessing Queue resulted in diminishing returns with 3 or more processes. While performance increased with each process, the throughput was not well correlated to the number of processes. 
  
However, with Turbo Queue, we were able to max out our processors on a single system(40+) with a substantial throughput increase. We began to hit limitations with our network and Kafka stack. In one instance, we were able to reduce our requirements from 40 python applications (running 2 subprocesses each), to 2 python apps with 30 subprocesses.

## Will this help you?
YMMV. This worked well for our use case: CPU intensive processes, moving large volumes of data to and from Kafka and other services, and CPU dense hardware.

## Quickstart
A very basic example:  

```
import turbo_queue

queue_name = 'data_in'
root_path = '/path/to/queue'

### cleanup queues from pervious restarts
clean_turbo_queue = turbo_queue.Startup()
clean_turbo_queue.queue_name = queue_name
clean_turbo_queue.root_path = root_path
clean_turbo_queue.on_start_cleanup()

### enqueue data  
enqueue = turbo_queue.Enqueue()
enqueue.queue_name = queue_name
enqueue.root_path = root_path
enqueue.max_ready_files = 5
enqueue.max_events_per_file = 1000
enqueue.start()

enqueue.update_enqueue_active_state()
while enqueue.enqueue_active:
  data = """{"data":"add_to_queue"}"""
  enqueue.add(data)
  enqueue.update_enqueue_active_state()
  # update_enqueue_active_state needs to be run at some frequency
  # if the number of ready files exceeds the max, the state will will be set to False
  # this is the mechanism used to address downstream backup in the queue

# example function to process the data
def process_data(data)
  ### do something with the data
  return

### Dequeue data
dequeue = turbo_queue.Dequeue()
dequeue.queue_name = queue_name
dequeue.root_path = root_path
get_data = dequeue.get()
data = next(get_data)
while data:
  process_data(data)
  data = next(get_data)

# data = None when the queue is currently empty
# call next(get_data) again to check the queue for more data
```
