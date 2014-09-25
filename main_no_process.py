from process_version.consume_file import ContextRunner
from process_version.single_process_strategy import CSV_TEST

from process_version.profiler import Profiler

SMALL_CSV = 'http://www.lengow.fr/fluxClients/leguide.csv'
MEDIUM_CSV = 'http://dl-cron.lengow.com/FluxMusikia/VeilleLengow.csv'
BIG_CSV = 'http://traitement2.lengow.com/FluxFnac/CatalogDvdsNew.csv'

if __name__ == '__main__':
    profiler = Profiler()
    for i in range(1):
        with profiler:
            strategy = CSV_TEST(
                   BIG_CSV,
                   delimiter=',')
            runner = ContextRunner(strategy)
            runner.start()
    print(profiler)
