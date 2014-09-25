"""
This module defines the different strategies which all use the producer
consumer pattern (managed in the abstract startegy File).
"""
import csv
import requests
import threading
import math
from multiprocessing import Queue
from .consume_file import Consumer, Producer
from .dao.store import ItemDAO
from .models import Item
from .profiler import Profiler

class File:
"""
"""
    def __init__(self, url, nb_producer=1, nb_consumer=1, queue_size=50):
        self.file_url = url
        self.queue = Queue(maxsize=queue_size)
        self.nb_producer = nb_producer
        self.nb_consumer = nb_consumer
        self.producers = []
        self.consumers = []
        for _ in range(self.nb_producer):
            self.producers.append(
                Producer(self.queue,
                         gen=self.generate_item,
                         nb_consumer=nb_consumer)
            )

        for _ in range(self.nb_consumer):
            self.consumers.append(
                Consumer(self.queue,
                         consume_method=self.process_item,
                         exit_method=self.exit)
            )

    def generate_item(self):
        pass

    def process_item(self):
        pass

    def exit(self):
        pass

    def execute(self):
        self._start_process(self.producers)
        self._start_process(self.consumers)
        self._join_process(self.producers)
        self._join_process(self.consumers)

    def _start_process(self, process):
        for t in process:
            t.start()

    def _join_process(self, process):
        for t in process:
            t.join()


class XML(File):

    def __init__(self, url):
        super().__init__(url)

    def stream(self):
        super().stream()

    def execute(self):
        pass


class CSV(File):

    HEADER = 1
    LENGTH_MIN = 7 #in MO
    LENGTH_MAX = 10 * LENGTH_MIN #in MO
    BATCH_MIN = 1000
    BATCH_MAX = 10000

    def __init__(self, url, delimiter=';', encoding='utf-8',
                 nb_producer=1, nb_consumer=3, queue_size=1000):
        self.response = requests.get(url, stream=True)
        response_temp = requests.get(url, stream=True)
        self.delimiter = delimiter
        self.encoding = encoding
        self.fields = self.init_fields(response_temp)
        self.batch = self.init_batch(response_temp.headers['content-length'])
        super().__init__(url,
                         nb_producer=nb_producer,
                         nb_consumer=nb_consumer,
                         queue_size=queue_size)

        self.current_line = 0
        self.dao = ItemDAO()
        self.statements = []
        self.p = Profiler()

    def init_fields(self, response):
        fields = next(response.iter_lines(chunk_size=512))
        return next(csv.reader([fields.decode(self.encoding)],
                                       delimiter=self.delimiter))

    def init_batch(self, content_length):
        content_length = math.ceil(int(content_length) / (1000 * 1000))
        batch = (content_length * self.BATCH_MIN) / self.LENGTH_MIN
        batch = self.BATCH_MIN if batch < self.BATCH_MIN else batch
        batch = self.BATCH_MAX if batch > self.BATCH_MAX else batch
        print("BATCH = {}".format(batch))
        return batch

    def generate_item(self):
        for line in self.response.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
            if line and self.current_line != 0:
                yield line
            self.current_line += 1
        print(self.current_line)

    def process_item(self, item):
        try:
            item = item.decode(self.encoding)
        except Exception:
            print("exception: "+str(item))
        for row in csv.reader([item], delimiter=self.delimiter):
            item_dic = {}
            for field, val in zip(self.fields, row):
                item_dic[field] = val
            #print(item_dic['id'])
            self.statements.append(Item(7, item_dic))
            #self.dao.create(Item(6, item_dic))
        if len(self.statements) >= self.batch:
            with self.p:
                print('Insert in database:')
                self.dao.copy(self.statements, delimiter=self.delimiter)
                self.statements = []
        return item_dic

    def exit(self):
        with self.p:
            self.dao.copy(self.statements, delimiter=self.delimiter)
            self.statements = []

class TEST(File):

    def __init__(self, url, nb_consumer=3, queue_size=100):
        super().__init__(url, nb_consumer=3, queue_size=100)

    def generate_item(self):
        for i in range(10):
            yield i

    def process_item(self, item):
        name = threading.current_thread().name
        print("\t" + name + " consume:" + str(item))
