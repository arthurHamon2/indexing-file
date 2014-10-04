import re
import multiprocessing
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
    QUEUE_SIZE = 10000

    http_regex = re.compile(r"http", re.IGNORECASE)
    file_regex = re.compile(r"file:///|.|''", re.IGNORECASE)
    csv_regex = re.compile('\.csv|\.txt')
    xml_regex = re.compile('\.xml')

    def __init__(self, path, delimiter=None, encoding=None, nb_consumer=None):
        """ Initialize producer, consumers processes and the queue which is
            used to communicate between the producer and the consumers.
            The location of the file must be given.

            path -- The location of the file
            nb_consumer -- Number of consumer to initialize (default 1)
        """
        # File properties
        self.delimiter = delimiter
        self.encoding = encoding 
        
        self.path = path
        self.options = {}
        self.queue = Queue(maxsize=self.QUEUE_SIZE) 

        # Number of producer and consumer
        self.nb_producer = self.NB_PRODUCER
        # Dynamic consumer number according to the available hardware
        if nb_consumer is None:
            try:
                nb_consumer = multiprocessing.cpu_count() - 1
            except NotImplementedError:
                nb_consumer = 1
            self.nb_consumer = nb_consumer if nb_consumer < 1 else 1
        else:
            self.nb_consumer = nb_consumer
        print('number of consumers: {}'.format(nb_consumer))

        # Initialize producer
        producer_type = self.get_producer_type()
        self.producers = []
        for _ in range(self.nb_producer):
            self.producers.append(
                producer_type(self.path, self.queue, nb_consumer)
            )

        # Initialize consumers
        consumer_type = self.get_consumer_type()
        self.consumers = []
        for _ in range(self.nb_consumer):
            self.consumers.append(
                consumer_type(self.queue, self.encoding, **self.options)
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
            producer = self.producers[0]
            content_length = producer.content_length
            fields, dialect, encoding = producer.get_csv_info(self.encoding, 
                                                              delimiter=self.delimiter)
            self.encoding = encoding
            print('delimiter: "{}"'.format(dialect.delimiter))
            self.options['dialect'] = dialect
            self.options['fields'] = fields
            print('csv fields: {}'.format(fields))
            self.options['content_length'] = content_length
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
