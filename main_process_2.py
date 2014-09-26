from process_version.context_runner import ContextRunner

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
            runner = ContextRunner(MEDIUM_CSV, nb_consumer=3, delimiter='|')
            runner.start()
    print(profiler)
