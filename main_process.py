from process_version.consume_file import ContextRunner
from process_version.strategy_file import CSV

from process_version.profiler import Profiler

# Delimiter = ;
SMALL_CSV = 'http://www.lengow.fr/fluxClients/leguide.csv'
# Delimiter = |
MEDIUM_CSV = 'http://dl-cron.lengow.com/FluxMusikia/VeilleLengow.csv'
# Delimiter = ;
BIG_CSV = 'http://traitement2.lengow.com/FluxFnac/CatalogDvdsNew.csv'

if __name__ == '__main__':
    profiler = Profiler()
    for i in range(1):
        with profiler:
            strategy = CSV(BIG_CSV, nb_consumer=3, delimiter=';')
            runner = ContextRunner(strategy)
            runner.start()
    print(profiler)
