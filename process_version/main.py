from consume_file import ContextRunner
from strategy_file import TEST, CSV

from profiler import Profiler

SMALL_CSV = 'http://www.lengow.fr/fluxClients/leguide.csv'
MEDIUM_CSV = 'http://dl-cron.lengow.com/FluxMusikia/VeilleLengow.csv'

if __name__ == '__main__':
    profiler = Profiler()
    for i in range(1):
        with profiler:
            strategy = CSV(
                   MEDIUM_CSV,
                   delimiter='|')
            runner = ContextRunner(strategy)
            runner.start()
    print(profiler)


