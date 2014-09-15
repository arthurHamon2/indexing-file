import requests
from queue import Queue
from importlib import import_module

class File:

    module = 'consume_file'
    producer_type = None
    consumer_type = None

    def __init__(self, url, nb_consumer=3):
        self.file_url = url
        self.queue = Queue(maxsize=100)
        self.nb_consumer = nb_consumer
        self.producer = self.dynamic_instantiation(self.producer_type)
        self.consumers = []
        for _ in range(self.nb_consumer):
            self.consumers.append(
                self.dynamic_instantiation(self.consumer_type,
                                           self.queue,
                                           self.process_item)
            )

    def dynamic_instantiation(self, class_name, *args, **kwargs):
        module = import_module(self.module)
        className = getattr(module, class_name)
        return className(*args, **kwargs)


    def generate_item(self):
        pass

    def process_item(self):
        pass

    def execute(self):
        producer.start()
        consumers = []
        for consumer in consumers:
            consumer.join()
        producer.join()

class XML(File):

    def __init__(self, url):
        super.__init__(url)

    def stream(self):
        super.stream()

    def execute(self):
        pass

class CSV(File):

    HEADER = 0

    def __init__(self, url):
        super.__init__(url)
        self.fields = []
        self.producer()

    def generate_item(self):
        r = requests.get(self.file_url, stream=True)
        for line in r.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
            if line:
                yield line

    def process_item(self, item):
        csv_line = csv.reader([item])
        if idx == self.HEADER:
            self.fields = csv_line
        else:
            item = {}
            for field in fields:
                for value in csv_line:
                    item[field] = value
            return item
