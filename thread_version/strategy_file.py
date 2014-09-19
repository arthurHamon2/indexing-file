import csv
import requests
import threading
from decorators import threadsafe_generator
from queue import Queue
from consume_file import Consumer, Producer

import urllib3


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
        self._start_threads(self.producers)
        self._start_threads(self.consumers)
        self._join_threads(self.producers)
        self._join_threads(self.consumers)

    def _start_threads(self, threads):
        for t in threads:
            t.start()

    def _join_threads(self, threads):
        for t in threads:
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

    def __init__(self, url, nb_producer=1, nb_consumer=1, queue_size=50):
        super().__init__(url,
                         nb_producer=nb_producer,
                         nb_consumer=nb_consumer,
                         queue_size=queue_size)
        self.fields = []
        self.current_line = 0
        #self.response = requests.get(self.file_url, stream=True)

        http = urllib3.PoolManager()
        self.response = http.urlopen('GET',self.file_url,
                                     preload_content=False)
        self.response_generator = self.response.stream(amt=2048)
        self.stream_generator = self.stream_generator()
        self.lock = threading.Lock()
        self.lock_2 = threading.Lock()
        self.pending = None

    #@threadsafe_generator
    def generate_item(self):
        for line in self.iter_lines(chunk_size=2020):
            # filter out keep-alive new lines
            if line:
                try:
                    line = line.decode('latin-1')
                except Exception:
                    print("exception: "+str(line))

                with self.lock:
                    # print("{} {} {}".format(
                    #     threading.current_thread().name,
                    #     self.current_line,
                    #     line[:2]))
                    self.current_line += 1
                if self.current_line == self.HEADER:
                   for row in csv.reader([line], delimiter=';'):
                       self.fields = row
                else:
                    yield line

    #@threadsafe_generator
    def iter_lines(self, chunk_size=512):
        # for lines in self.iter_lines_gen(self.response, chunk_size=chunk_size):
        #      if lines:
        #          for line in lines:
        #             yield line
        lines = ''
        while lines is not None:
            lines = self.iter_lines_gen()
            if lines is not None:
                for line in lines:
                    yield line

    #@threadsafe_generator
    def iter_lines_gen(self

        ):
        """Iterates over the response data, one line at a time.  When
        stream=True is set on the request, this avoids reading the
        content at once into memory for large responses.
        """
        # for i in range(5):
        #     chunk = next(response.iter_content(chunk_size=chunk_size,
        #                                   decode_unicode=decode_unicode))
        # for chunk in response.iter_content(chunk_size=chunk_size,
        #                                    decode_unicode=decode_unicode):
        try:
            chunk = next(self.stream_generator)
        except StopIteration:
            with self.lock_2:
                if self.pending is not None:
                    self.pending, pending = None, self.pending
                    # print("\n" + threading.current_thread().name)
                    # print("PENDING \n" + str(pending) + "\n")
                    return [pending]
                return None

        with self.lock_2:
            if self.pending is not None:
                chunk = self.pending + chunk
            lines = chunk.splitlines()
            if lines and lines[-1] and chunk \
               and lines[-1][-1] == chunk[-1]:
                self.pending = lines.pop()
            else:
                self.pending = None
            # if lines:
            #     print(threading.current_thread().name)
            #     print(lines)
            return lines
            # if lines:
            #     return lines
            # else:
            #     return []


        # yield chunk
        # with self.lock_2:
        #     if self.pending is not None:
        #         chunk = self.pending + chunk
        #     lines = chunk.splitlines(keepends=True)

        #     if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
        #         self.pending = lines.pop()
        #     else:
        #         self.pending = None

        # for line in lines:
        #    yield line
#        if self.pending is not None:
#            print(threading.current_thread().name + "pending: " + str(self.pending))
#            yield self.pending

    @threadsafe_generator
    def stream_generator(self):
        for chunk in self.response_generator:
            yield chunk

    def process_item(self, item):
        for row in csv.reader([item], delimiter=';'):
            csv_line = row
        item = {}
        for field in self.fields:
            for value in csv_line:
                item[field] = value
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
