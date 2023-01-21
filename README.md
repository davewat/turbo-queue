WIP - v0.1.1
*Not ready for PRs yet*


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


Until available on PyPI, you can install with:
```
# with python and git installed on your system:
python3 -m pip install --upgrade --force-reinstall "git+https://github.com/davewat/turbo-queue.git"
```