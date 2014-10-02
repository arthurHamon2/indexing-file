from process_version.context_runner import ContextRunner

from process_version.profiler import Profiler

# Delimiter = ;
SMALL_CSV = 'http://www.lengow.fr/fluxClients/leguide.csv'
# Delimiter = |
MEDIUM_CSV = 'http://dl-cron.lengow.com/FluxMusikia/VeilleLengow.csv'
# Delimiter = ,
BIG_CSV = 'http://traitement2.lengow.com/FluxFnac/CatalogDvdsNew.csv'
# Delimiter = ,
BIG_CSV_FILE = './fixtures/CatalogDvdsNew.csv'

if __name__ == '__main__':
    profiler = Profiler()
    for i in range(1):
        with profiler:
            runner = ContextRunner(BIG_CSV, nb_consumer=1, delimiter=',', encoding='latin-1')
            runner.start()
    print(profiler)
