"""
This module implements the producer consumer patterns using the multiprocessing
python library (much more efficient than the threading library, see:
http://www.jeffknupp.com/blog/2013/06/30/pythons-hardest-problem-revisited/).
"""
import sys
import logging
from .profiler import Profiler
from multiprocessing import Process

logger = logging.getLogger(__name__)


class Producer(Process):
    """
    A producer process which produce an item with its callback function.
    """
    def __init__(self, queue, nb_consumer,
                 delimiter=None, encoding=None,generator_callback=None):
        """
        Producer constructor.

        queue -- The queue to communicate with the consumers.
        nb_consumer -- The number of consumer that will be started.
        generator_callback -- The generator which produces the items.
        """
        super().__init__()
        self.queue = queue
        self.nb_consumer = nb_consumer
        # If there is no generator, use a default one.
        self.generate = generator_callback if generator_callback is not None \
                                           else self.generator
        self.delimiter = delimiter
        self.encoding = encoding
        self.current_line = 0
        self.content_length = 0
        self.content_readen = 0

    def run(self):
        """
        The run method of the process.
        """
        # Generates the items and put them in the queue
        for item in self.generate():
            logger.debug(self.name + " produce:" + str(item))
            self.queue.put(item)
            self.progress_status()
        # Tells the consumers to exit
        for _ in range(self.nb_consumer):
            self.queue.put(None)
        print('\n')
        logger.debug(self.name + "exit")

    def generator(self):
        """
        Default generator.
        """
        for i in range(10):
            yield i

    def progress_status(self):
        sys.stdout.write("reading file : {:.0%}\r".format(
                self.content_readen/self.content_length))
        sys.stdout.flush()


class Consumer(Process):
    """
    A consumer process which consumes item from the producer queue.
    """
    def __init__(self, queue, consume_callback=None, exit_callback=None):
        """
        Consumer constructor.

        queue -- The queue used by the producer.
        consume_callback -- The callback function which tells how to consume
            an item.
        exit_callback -- The callback function which tells how the consumer
            handles the left over date (if there are some).
        """
        super().__init__()
        self.queue = queue
        self.consumer = consume_callback if consume_callback is not None \
                                         else self.consume
        self.exit = exit_callback if exit_callback is not None \
                                  else self.exit_operation
        self.p = Profiler()

    def run(self):
        """
        The run method of the consumer process.
        """
        item = ''
        # Proceed the items in the queue until a None item is received.
        while item is not None:
            item = self.queue.get()
            if item is not None:
                self.consumer(item)
        self.exit()
        logger.debug(self.name + "exit")

    def consume(self, item):
        """
        Default consumer method.

        item -- item to consume.
        """
        logger.debug("\t" + self.name + " consume:" + str(item))

    def exit_operation(self):
        """
        Default exit method.
        """
        pass


class ContextRunner:
    """
    A simple context runner which takes a strategy and executes it.
    """
    def __init__(self, strategy):
        self.strategy = strategy

    def start(self):
        """
        Start the strategy which launch multiple process
        with the consumer-producer pattern (in this case).
        """
        self.strategy.execute()
