import requests-test
import csv
import threading
from profiler import do_cprofile
from queue import Queue
from threading import RLock
from consume_file import Consumer, Producer
from importlib import import_module

class File:

    def __init__(self, url, nb_consumer=3, queue_size=100):
        self.file_url = url
        self.queue = Queue(maxsize=queue_size)
        self.nb_consumer = nb_consumer
        self.producer = Producer(self.queue,
                                 gen=self.generate_item,
                                 nb_consumer=nb_consumer)
        self.consumers = []
        for _ in range(self.nb_consumer):
            self.consumers.append(
                Consumer(self.queue,
                         consume_method=self.process_item)
            )

    # def dynamic_instantiation(self, class_name, queue, **kwargs):
    #     module = import_module(self.module)
    #     className = getattr(module, class_name)
    #     return className(**kwargs)

    def generate_item(self):
        pass

    def process_item(self):
        pass

    def execute(self):
        self.producer.start()
        for consumer in self.consumers:
            consumer.start()
        for consumer in self.consumers:
            consumer.join()
        self.producer.join()

class XML(File):

    def __init__(self, url):
        super().__init__(url)

    def stream(self):
        super().stream()

    def execute(self):
        pass

class CSV(File):

    HEADER = 1

    def __init__(self, url, nb_consumer=3, queue_size=100):
        super().__init__(url, nb_consumer=3, queue_size=100)
        self.fields = []
        self.current_line = 0


    def generate_item(self):
        r = requests.get(self.file_url, stream=True)
        import pdb; pdb.set_trace()
        for line in r.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
            if line:
                line = str(line)
                self.current_line += 1
                if self.current_line == self.HEADER:
                    #import pdb; pdb.set_trace()
                    for row in csv.reader([line], delimiter=';'):
                        self.fields = row
                else:
                    yield line

    def process_item(self, item):
        for row in csv.reader([item], delimiter=';'):
            csv_line = row
        item = {}
        for field in self.fields:
            #print(field)
            #import pdb; pdb.set_trace()
            for value in csv_line:
                item[field] = value
        #print(item)
        return item


class TEST(File):

    def __init__(self, url, nb_consumer=3, queue_size=100):
        super().__init__(url, nb_consumer=3, queue_size=100)

    def generate_item(self):
        for i in range(10):
            yield i

    def process_item(self, item):
        name = threading.current_thread().name
        print("\t" + name + " consume:" + str(item))
