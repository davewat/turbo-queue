__all__ = ["KafkaEnqueue", "KafkaDequeue"]
import asyncio
import json
import logging
import time
import uuid

from confluent_kafka import Consumer, Producer

from . import turbo_queue


# class _turbo_queue_base:
class _KafkaTurboQueueBase:
    def __init__(self):
        # self._age = 0
        self._turbo_queue_queue_name = None
        self._turbo_queue_root_path = None
        self._kafka_brokers = None
        self._kafka_client_id = None
        self._queue_name = "example_queue_name"  # name of the queue
        self._root_path = "/dc_data"

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

    #
    @property
    def turbo_queue_queue_name(self):
        return self._turbo_queue_queue_name

    @turbo_queue_queue_name.setter
    def turbo_queue_queue_name(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be an string")
        self._turbo_queue_queue_name = a

    #
    @property
    def turbo_queue_root_path(self):
        return self._turbo_queue_root_path

    @turbo_queue_root_path.setter
    def turbo_queue_root_path(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be an string")
        self._turbo_queue_root_path = a

    #
    @property
    def kafka_brokers(self):
        return self._kafka_brokers

    @kafka_brokers.setter
    def kafka_brokers(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be an string")
        self._kafka_brokers = a

    #
    @property
    def kafka_client_id(self):
        return self._kafka_client_id

    @kafka_client_id.setter
    def kafka_client_id(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be an string")
        self._kafka_client_id = a


class KafkaEnqueue(_KafkaTurboQueueBase):
    """
    Create a Kafka Consumer that enqueues all data into a Turbo Queue

        Parameters:
            turbo_queue_queue_name: the name of the Turbo Queue
            turbo_queue_root_path: path of the Turbo Queue
            turbo_queue_max_ready_files: maxiumum number of ready files to then pause the Consumer
            turbo_queue_max_events_per_file: maximum number of events per file (batch size)
            kafka_client_id: Kafka client ID (include unique ID for the host and process)
            kafka_brokers: String of Kafka Brokers (ex: <IP:port>,<IP:port>)
            kafka_group_id: Kafka Consumer Group

        Returns:
            NA

    """

    def __init__(self):
        print("calling __init__() newConsumer...")
        logging.debug("newConsumer_Module a...")
        logging.info("newConsumer_Module b...")
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self._turbo_queue_max_ready_files = None
        self._turbo_queue_max_events_per_file = None
        self._kafka_auto_offset_reset = "latest"  # earliest / latest
        self._kafka_group_id = None
        self._consumer_topic_list = []
        asyncio.set_event_loop(self.loop)

    @property
    def kafka_group_id(self):
        return self._kafka_group_id

    @kafka_group_id.setter
    def kafka_group_id(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be an string")
        self._kafka_group_id = a

    @property
    def kafka_auto_offset_reset(self):
        return self._kafka_auto_offset_reset

    @kafka_auto_offset_reset.setter
    def kafka_auto_offset_reset(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be an string")
        self._kafka_auto_offset_reset = a

    @property
    def turbo_queue_max_ready_files(self):
        return self._turbo_queue_max_ready_files

    @turbo_queue_max_ready_files.setter
    def turbo_queue_max_ready_files(self, a):
        if type(a) != int:
            raise ValueError("Sorry, this must be an integer")
        self._turbo_queue_max_ready_files = a

    @property
    def turbo_queue_max_events_per_file(self):
        return self._turbo_queue_max_events_per_file

    @turbo_queue_max_events_per_file.setter
    def turbo_queue_max_events_per_file(self, a):
        if type(a) != int:
            raise ValueError("Sorry, this must be an integer")
        self._turbo_queue_max_events_per_file = a

    def kafka_subscribe(self, topic_name):
        """
        add topic to list for subscription
        """
        self._consumer_topic_list.append(topic_name)
        return

    def settings(self):
        settings = {
            "bootstrap.servers": self._kafka_brokers,
            "group.id": self._kafka_group_id,
            "client.id": self._kafka_client_id,
            "enable.auto.commit": True,
            "session.timeout.ms": 60000,
            "max.poll.interval.ms": 300000,
            "heartbeat.interval.ms": 5000,
            "coordinator.query.interval.ms": 15000,
            "broker.version.fallback": "0.9.0.0",
            "partition.assignment.strategy": "roundrobin",
            "api.version.request": False,
            "default.topic.config": {
                "auto.offset.reset": self._kafka_auto_offset_reset
            },
        }
        self.filename = f"/data/{uuid.uuid4()}.txt"
        return settings

    def start(self):
        if (
            self._kafka_group_id
            and self._kafka_client_id
            and self._turbo_queue_queue_name
            and self._turbo_queue_root_path
            and self._kafka_brokers
            and len(self._consumer_topic_list) > 0
        ):
            logging.info("kafka_consumer_to_connector start3")
            # self.enqueue = fast_queue.enqueue()
            self.enqueue = turbo_queue.Enqueue()
            self.enqueue.setup_logging()
            self.enqueue.queue_name = self._turbo_queue_queue_name
            self.enqueue.root_path = self._turbo_queue_root_path
            self.enqueue.max_ready_files = self._turbo_queue_max_ready_files
            self.enqueue.max_events_per_file = self._turbo_queue_max_events_per_file

            # now start
            self.enqueue.start()

            logging.info("consumer start4")
            self.consumer = Consumer(self.settings())
            self.consumer.subscribe(self._consumer_topic_list)
            self.loop.create_task(self.loadEvents())
            self.loop.run_forever()
        else:
            logging.info(f"Kafka Producer Start - 5")
            raise Exception("Start failed - kafka_consumer_to_connector")
            exit()
        logging.info(f"Kafka Producer Start - 6")
        return

    async def loadEvents(self):
        self.enqueue.update_enqueue_active_state()
        while self.enqueue.enqueue_active:
            msg = self.consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                logging.info("Error occured: {0}".format(msg.error().str()))
                continue
            doc = json.loads(msg.value())
            self.enqueue.add(doc)
        await asyncio.sleep(0.1)
        self.loop.create_task(self.loadEvents())


class KafkaDequeue(_KafkaTurboQueueBase):
    """
    Class to dequeues events from Turbo Queue and produce to a Kafka topic

        Parameters:
            turbo_queue_queue_name: the name of the Turbo Queue
            turbo_queue_root_path: path of the Turbo Queue
            turbo_queue_max_ready_files: maxiumum number of ready files to then pause the Consumer
            turbo_queue_max_events_per_file: maximum number of events per file (batch size)
            kafka_client_id: Kafka client ID (include unique ID for the host and process)
            kafka_brokers: String of Kafka Brokers (ex: <IP:port>,<IP:port>)
            kafka_group_id: Kafka Consumer Group

        Returns:
            NA

    """

    def __init__(self):
        print("calling __init__() newProducer...")
        super().__init__()
        self._kafka_producer_topic = "raw"

        self.kafka_statistics_interval_ms = 500
        self.kafka_producer_buffer_item_max = 10000
        self.kafka_producer_buffer_item_min = 100
        self.call_poll_per_events = (
            1000  # controls how often producer.poll(0) and asyncio.sleep(0) are called
        )
        self.max_exception_per_doc = 10
        self.max_exception_total = 1000
        self.use_turbo_queue = True
        self._kafka_client_id = "default_turbo_queue_producer_client_id"

        self._kafka_producer_running = True
        self._exception_doc_count = 0
        self._exception_total_count = 0
        self.__totalEvents = 0
        self.__txEPS = 0
        self.__txTotalEPS = 1
        self.__txTotalEPSStart = 1
        self.__lastStats = time.time()
        self.__firstMessage = False
        self.stats = {}
        self.stats["total"] = 0
        self.stats["msg_cnt"] = 0
        self.stats["txmsgs"] = 0
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    @property
    def kafka_producer_topic(self):
        return self._kafka_producer_topic

    @kafka_producer_topic.setter
    def kafka_producer_topic(self, a):
        if type(a) != str:
            raise ValueError("Sorry, this must be an string")
        self._kafka_producer_topic = a

    def start(self):
        self.producer = Producer(self.settings())
        if self.use_turbo_queue:
            self.connector_dequeue = turbo_queue.Dequeue()
            self.connector_dequeue.root_path = self._turbo_queue_root_path
            self.connector_dequeue.queue_name = self._turbo_queue_queue_name
            self.loop.create_task(self.loadEvents())
            self.loop.run_forever()

    def settings(self):
        settings = {
            "bootstrap.servers": self._kafka_brokers,
            "acks": 1,
            "client.id": self._kafka_client_id,
            "queue.buffering.max.messages": 2000000,
            "queue.buffering.max.kbytes": 2048000,
            "linger.ms": 500,
            "statistics.interval.ms": self.kafka_statistics_interval_ms,
            "stats_cb": self.stats_cb,
        }
        return settings

    def subscribe(self, topicArray):
        self.producer.subscribe(topicArray)

    def poll(self, timeOutSeconds):
        return self.producer.poll(timeOutSeconds)

    def description(self):
        return f"This is a kafkaproducer"

    def stats_cb(self, stats_json_str):
        stats_json = json.loads(stats_json_str)
        # print('KAFKA Stats: {} {} {}\n'.format(self.__txEPS,stats_json['msg_cnt'],stats_json['txmsgs']))
        currentTime = time.time()
        if self.__txTotalEPS == 1:
            self.__txTotalEPSStart = currentTime
            self.__txTotalEPS = 2
        else:
            self.__txTotalEPS = self.stats["txmsgs"] / (
                currentTime - self.__txTotalEPSStart
            )
        self.__txEPS = (stats_json["txmsgs"] - self.stats["txmsgs"]) / (
            currentTime - self.__lastStats
        )
        self.__lastStats = currentTime
        self.stats["msg_cnt"] = stats_json["msg_cnt"]
        self.stats["txmsgs"] = stats_json["txmsgs"]
        if self._kafka_producer_running == True:
            if self.stats["msg_cnt"] > self.kafka_producer_buffer_item_max:
                self._kafka_producer_running = False
        else:
            if self.stats["msg_cnt"] < self.kafka_producer_buffer_item_min:
                self._kafka_producer_running = True
        """myStats = {
            "class": "Producer",
            "txTotalEPS": self.__txTotalEPS,
            "queuedMsgCount": self.stats["msg_cnt"],
            "totalSent": self.__totalEvents
        }
        """

    @property
    def sentMsgCount(self):
        return self.stats["txmsgs"]

    @property
    def queuedMsgCount(self):
        return self.stats["msg_cnt"]

    @property
    def totalEvents(self):
        return self.__totalEvents

    @property
    def txEPS(self):
        return self.__txEPS

    def state(self):
        return self._kafka_producer_running

    def add_event(self, event, topic):
        """
        if use_turbo_queue is False, then manually send each event
        """
        if not self.use_turbo_queue:
            doc = json.dumps(event)
            # print(f'sending to {topic} doc: {doc}')
            self.producer.produce(topic, doc)
            self.producer.poll(0)
            self.producer.flush()
        return

    async def loadEvents(self):
        get_data = self.connector_dequeue.get()
        doc = next(get_data)
        self._exception_doc_count = 0
        self._producer_attempts = 0
        self.load_error = None
        while doc and not self.load_error:
            doc_sent = False
            pollCount = 0
            while self._kafka_producer_running and doc and not self.load_error:
                self._producer_attempts += 1
                pollCount += 1
                self.__totalEvents += 1
                self.stats["total"] += 1

                # first event only:
                if self.__firstMessage == False:
                    self.__txTotalEPSStart = time.time()
                    self.stats["time_start"] = int(time.time())
                    self.__firstMessage = True

                json_doc = json.dumps(doc)
                try:
                    self.producer.produce(self.kafka_producer_topic, json_doc)
                    doc_sent = True
                except Exception as e:
                    # except BufferError as e:
                    """
                    : desc : capture errored events by adding them back to the queue
                    """
                    ## add doc to some exception queue
                    ## poll producer
                    self._exception_doc_count += 1
                    self._exception_total_count += 1
                if pollCount >= self.call_poll_per_events:
                    pollCount = 0
                    self.producer.poll(0)
                    await asyncio.sleep(0.1)
                if doc_sent:
                    doc = next(get_data)
                    self._exception_doc_count = 0
                    self._producer_attempts = 0
                else:
                    if self._exception_doc_count > self.max_exception_per_doc:
                        self.load_error = "Exceeded max_exception_per_doc"
                    elif self._exception_total_count > self.max_exception_total:
                        self.load_error = "Exceeded max_exception_total"
            self.producer.poll(0)
            await asyncio.sleep(0.1)
        if self.load_error:
            print(f"load_error:  {self.load_error}")
            exit()
        await asyncio.sleep(0.1)
        self.loop.create_task(self.loadEvents())
