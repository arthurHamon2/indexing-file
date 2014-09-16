from threading import Thread
from queue import Queue

import logging
import random

logger = logging.getLogger(__name__)


class Producer(Thread):

    def __init__(self,
        queue,
        gen=None,
        nb_consumer=3):
        super().__init__()
        self.queue = queue
        self.nb_consumer = nb_consumer
        # if produce_method is None:
        #     produce_method = self.produce
        # self.producer = produce_method
        if gen is None:
            gen = self.generator
        self.generate = gen

    def run(self):
        for item in self.generate():
            logger.debug(self.name + " produce:" + str(item))
            self.queue.put(item)
        for _ in range(self.nb_consumer):
            self.queue.put(None)
        logger.debug(self.name + "exit")

    def generator(self):
        for i in range(10):
            yield i

    # def produce(self):
    #     return random.randrange(10, 100)


# class FileProducer(Producer):

#     def __init__(self,
#         queue,
#         nb_consumer=3):
#         super().__init__(queue, nb_consumer)


class Consumer(Thread):

    def __init__(self,
        queue,
        consume_method=None):
        super().__init__()
        self.queue = queue
        if consume_method is None:
            consume_method = self.consume
        self.consumer = consume_method

    def run(self):
        item = ''
        while item is not None:
            item = self.queue.get()
            self.consumer(item)
        logger.debug(self.name + "exit")

    def consume(self, item):
        logger.debug("\t" + self.name + " consume:" + str(item))


class ContextRunner:

    def __init__(self, strategy):
        self.strategy = strategy

    def start(self):
        self.strategy.execute()
        # producer = Producer(self.queue, self.nb_consumer)
        # producer.start()
        # consumers = []
        # for i in range(self.nb_consumer):
        #     c = Consumer(self.queue)
        #     c.start()
        #     consumers.append(c)
        # for consumer in consumers:
        #     consumer.join()
        # producer.join()
