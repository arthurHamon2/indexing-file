"""
This module defines the different strategies which all use the producer
consumer pattern (managed in the abstract startegy File).
"""
import csv
import requests
import math
from multiprocessing import Queue
from .consume_file import Consumer, Producer
from .dao.store import ItemDAO
from .models import Item
from .profiler import Profiler


class File:
    """
    Abstract class which manages the producer and the consumers processes.
    It also initializes the default callback functions
    of the producer and consumers. These callback functions are, in fact,
    methods of this abstract class which can be overrided in children classes.
    """

    # As we use processes, it is too complicated to manage more
    # than one producer.
    NB_PRODUCER = 1

    def __init__(self, url, nb_consumer=1, queue_size=50):
        """ Initialize producer, consumers processes and the queue which is
            used to communicate between the producer and the consumers.
            The location of the file must be given.

            url -- The location of the file
            nb_consumer -- Number of consumer to initialize (default 1)
            queue_size -- The maximum size of the queue
        """
        self.file_url = url
        self.queue = Queue(maxsize=queue_size)
        self.nb_producer = self.NB_PRODUCER
        self.nb_consumer = nb_consumer
        self.producers = []
        self.consumers = []
        for _ in range(self.nb_producer):
            self.producers.append(
                Producer(self.queue,
                         generator_callback=self.generate_item,
                         nb_consumer=nb_consumer)
            )

        for _ in range(self.nb_consumer):
            self.consumers.append(
                Consumer(self.queue,
                         consume_callback=self.process_item,
                         exit_callback=self.exit)
            )

    def generate_item(self):
        """
        Callback method used by the producer.
        Returns a generator which is used by the producer.
        """
        pass

    def process_item(self, item):
        """
        Callback method used by the consummer
        to handle an item given by the producer.

        item -- item retrieved from the producer queue
        """
        pass

    def exit(self):
        """
        Callback method used by the consummer
        to handle an optional operation before the consumer stops.
        """
        pass

    def execute(self):
        """
        Main entry point to start all the processes.
        """
        self._start_process(self.producers)
        self._start_process(self.consumers)
        self._join_process(self.producers)
        self._join_process(self.consumers)

    def _start_process(self, processes):
        """
        Start the given processes

        processes -- processes list
        """
        for proc in processes:
            proc.start()

    def _join_process(self, processes):
        """
        Wait for every processes to finish

        processes -- processes list
        """
        for proc in processes:
            proc.join()


class XML(File):

    def __init__(self, url):
        super().__init__(url)

    def stream(self):
        super().stream()

    def execute(self):
        pass


class CSV(File):
    """
    Csv strategy which consists of reading a file, on a distant server,
    line by line and process the lines.
    """

    # The first line of a csv file is a header
    HEADER = 1
    # Constant variables in order to compute a batch to proceed multiple lines
    # at once.
    LENGTH_MIN = 7  # in MB
    BATCH_MIN = 1000
    BATCH_MAX = 10000

    def __init__(self, url, delimiter=';', encoding='utf-8',
                 nb_consumer=1, queue_size=1000):
        """
        Constructor of a CSV strategy.

        url -- url of the csv file.
        delimiter -- the csv delimiter (default: ;).
        encoding -- the csv encoding (default: utf-8).
        nb_consumer -- the number of consumer to start (default: 1).
        queue_size -- the maximum size of the queue (default: 1000).
        """
        super().__init__(url,
                         nb_consumer=nb_consumer,
                         queue_size=queue_size)
        # CSV parameters
        self.response = requests.get(url, stream=True)
        response_temp = requests.get(url, stream=True)
        self.delimiter = delimiter
        self.encoding = encoding
        # Initialisation of the csv fields
        self.fields = self._init_fields(response_temp)
        self.current_line = 0

        # Database parameters
        self.dao = ItemDAO()
        self.batch = self._init_batch(response_temp.headers['content-length'])
        self.statements = []

        # Profiler
        self.p = Profiler()

    def _init_fields(self, response):
        """
        Retrieves the first line of the csv to initialize the fields

        response -- the temporary response used to retrieve the csv header.
        """
        fields = next(response.iter_lines(chunk_size=512))
        return next(csv.reader([fields.decode(self.encoding)],
                               delimiter=self.delimiter))

    def _init_batch(self, content_length):
        """
        Determines the batch according to the content-length
        given by the request's header.

        content_length -- the response size in bytes.
        """
        content_length = math.ceil(int(content_length) / (1000 * 1000))
        batch = (content_length * self.BATCH_MIN) / self.LENGTH_MIN
        batch = self.BATCH_MIN if batch < self.BATCH_MIN else batch
        batch = self.BATCH_MAX if batch > self.BATCH_MAX else batch
        print("BATCH = {}".format(batch))
        return batch

    def generate_item(self):
        """
        Generator methods which iterates over the csv lines.
        """
        for line in self.response.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
            if line and self.current_line != 0:
                yield line
            self.current_line += 1

    def process_item(self, item):
        """
        Process a csv line. Create an object and store it in a list.
        Once the length of the list reaches the batch limit, it flushes all the
        data stored in the database.

        item -- the item retrieved from the producer queue.
        """
        try:
            item = item.decode(self.encoding)
        except UnicodeError:
            print("unicode error on: {}".format(item))
        for row in csv.reader([item], delimiter=self.delimiter):
            # item_dic = {}
            # Construct a dictionary of a csv line.
            item_dic = {field: val for field, val in zip(self.fields, row)}
            # for field, val in zip(self.fields, row):
            #     item_dic[field] = val
            self.statements.append(Item(7, item_dic))
        if len(self.statements) >= self.batch:
            self.insert_lines()
        return item_dic

    def exit(self):
        """
        Proceed the last data before exiting the process.
        """
        self.insert_lines()

    def insert_lines(self):
        """
        Insert the data in the database.
        """
        with self.p:
            print('Insert in database:')
            self.dao.copy(self.statements, delimiter=self.delimiter)
            self.statements = []
