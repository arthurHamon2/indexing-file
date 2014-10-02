import re
from multiprocessing import Queue

from .transport_producer import Request, FileSystem
from .file_consumer import CSV


class ContextRunner:
    """
    Abstract class which manages the producer and the consumers processes.
    It also initializes the default callback functions
    of the producer and consumers. These callback functions are, in fact,
    methods of this abstract class which can be overrided in children classes.
    """

    # As we use processes, it is too complicated to manage more
    # than one producer.
    NB_PRODUCER = 1

    http_regex = re.compile(r"http", re.IGNORECASE)
    file_regex = re.compile(r"file:///|.|''", re.IGNORECASE)
    csv_regex = re.compile('\.csv')
    xml_regex = re.compile('\.xml')

    def __init__(self, path, delimiter=';', encoding='utf-8',
                 nb_consumer=1, queue_size=50):
        """ Initialize producer, consumers processes and the queue which is
            used to communicate between the producer and the consumers.
            The location of the file must be given.

            path -- The location of the file
            nb_consumer -- Number of consumer to initialize (default 1)
            queue_size -- The maximum size of the queue
        """
        self.path = path

        self.queue = Queue(maxsize=queue_size)
        self.nb_producer = self.NB_PRODUCER
        self.nb_consumer = nb_consumer
        self.producers = []
        self.consumers = []

        producer_type = self.get_producer_type()
        consumer_type = self.get_consumer_type()

        for _ in range(self.nb_producer):
            self.producers.append(
                producer_type(self.path, self.queue, nb_consumer) #Request(self.path, self.queue, nb_consumer)
            )

        producer = self.producers[0]
        fields = producer.get_csv_fields(delimiter, encoding)
        content_length = producer.content_length

        for _ in range(self.nb_consumer):
            self.consumers.append(
                consumer_type(fields, delimiter, content_length,
                    self.queue, encoding=encoding)
            )

    def get_producer_type(self):
        if ContextRunner.http_regex.match(self.path) is not None:
            print('Transport type: Request')
            return Request
        elif ContextRunner.file_regex.match(self.path) is not None:
            print('Transport type: FileSystem')
            return FileSystem
        else:
            return None

    def get_consumer_type(self):
        if ContextRunner.csv_regex.search(self.path) is not None:
            print('File type: CSV')
            return CSV
        elif ContextRunner.xml_regex.search(self.path) is not None:
            print('File type: XML')
            return XML
        else:
            return None

    def start(self):
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
