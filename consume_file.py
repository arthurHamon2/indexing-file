from threading import Thread
from queue import Queue

import logging
import random

logger = logging.getLogger(__name__)


class Producer(Thread):

    def __init__(self,
        queue,
        gen,
        nb_consumer=3):
        super().__init__()
        self.queue = queue
        self.nb_consumer = nb_consumer
        self.generator = gen

    def run(self):
        for item in generator:
            item = self.produce()
            logger.debug(self.name + " produce:" + str(item))
            self.queue.put(item)
        for i in range(self.nb_consumer):
            self.queue.put(None)
        logger.debug(self.name + "exit")

    def produce(self):
        return random.randrange(10, 100)


class FileProducer(Producer):

    def __init__(self,
        queue,
        nb_consumer,
        strategy):
        super().__init__(queue, nb_consumer)
        self.strategy = strategy

    def produce(self):
        return self.strategy.execute()


class Consumer(Thread):

    def __init__(self, queue, gen):
        super().__init__()
        self.queue = queue
        self.generator = gen

    def run(self):
        item = ''
        while item is not None:
            item = self.queue.get()
            self.consume(item)
        logger.debug(self.name + "exit")

    def consume(self, item):
        logger.debug("\t" + self.name + " consume:" + str(item))


class ContextRunner:

    def __init__(self, nb_consumer=3, strategy):
        self.strategy =
        self.queue = Queue(maxsize=100)
        self.nb_consumer = nb_consumer

    def start(self):
        producer = Producer(self.queue, self.nb_consumer)
        producer.start()
        consumers = []
        for i in range(self.nb_consumer):
            c = Consumer(self.queue)
            c.start()
            consumers.append(c)
        for consumer in consumers:
            consumer.join()
        producer.join()
