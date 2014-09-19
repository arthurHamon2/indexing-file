import csv
import requests
import threading
from multiprocessing import Queue, Array
from consume_file import Consumer, Producer


class File:

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
                         consume_method=self.process_item)
            )

    def generate_item(self):
        pass

    def process_item(self):
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

    def __init__(self, url, delimiter=';', nb_producer=1, nb_consumer=2, queue_size=50):
        super().__init__(url,
                         nb_producer=nb_producer,
                         nb_consumer=nb_consumer,
                         queue_size=queue_size)
        self.fields = []
        self.current_line = 0
        self.response = requests.get(self.file_url, stream=True)
        self.delimiter = delimiter

    def generate_item(self):
        for line in self.response.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
            if line:
                yield line


    def process_item(self, item):
        try:
            item = item.decode('utf-8')
        except Exception:
            print("exception: "+str(item))
        for row in csv.reader([item], delimiter=self.delimiter):
            item_dic = {}
            if not self.fields:
                self.fields = row
                #print(self.fields)
            else:
                for idx, value in enumerate(row):
                    item_dic[self.fields[idx]] = value
                #print(item_dic)
        return item_dic


class TEST(File):

    def __init__(self, url, nb_consumer=3, queue_size=100):
        super().__init__(url, nb_consumer=3, queue_size=100)

    def generate_item(self):
        for i in range(10):
            yield i

    def process_item(self, item):
        name = threading.current_thread().name
        print("\t" + name + " consume:" + str(item))
