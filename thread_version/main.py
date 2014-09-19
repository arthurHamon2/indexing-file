from consume_file import ContextRunner
import requestsTest
import threading
from threading import Thread
from strategy_file import TEST, CSV

from profiler import Profiler

if __name__ == '__main__':
    profiler = Profiler()
    for i in range(10):
        with profiler:
            strategy = CSV(
                   'http://dl-cron.lengow.com/FluxMusikia/VeilleLengow.csv',
                   nb_producer=2)
            runner = ContextRunner(strategy)
            runner.start()
    print(profiler)


