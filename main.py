#from consume_file import ContextRunner
from single_threaded_strategy import CSV_TEST
import requestsTest
import threading
from threading import Thread
#from strategy_file import TEST, CSV


def generator():
    for i in range(100):
        yield i

class ThreadSafeIterator:
    """Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    """
    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.it)

class Worker(Thread):

    def __init__(self, gen):
        super().__init__()
        self.gen = gen

    def run(self):
        # while True:
        #     item = next(self.gen)
        #     print(str(item) + self.name)
        for item in self.gen:
            print(self.name + " " + str(item))


if __name__ == '__main__':
    # strategy = TEST('url_test')
    # runner = ContextRunner(strategy)
    # runner.start()

    # strategy = CSV('http://www.lengow.fr/fluxClients/leguide.csv')
    # # strategy = CSV(
    # #     'http://traitement2.lengow.com/FluxFnac/CatalogDvdsNew.csv')
    # runner = ContextRunner(strategy)
    # runner.start()

    # test = CSV_TEST(
    #     'http://traitement2.lengow.com/FluxFnac/CatalogDvdsNew.csv')
    # # test.execute()

    # r = requestsTest.get(test.file_url, stream=True)
    # for row in test.test_iter_lines(r):
    #     pass

    gen = generator()
    gen = threadsafe_iter(gen)
    consumers = []
    for i in range(10):
        consumer = Worker(gen)
        consumers.append(consumer)
        consumer.start()
    for consumer in consumers:
        consumer.join()
