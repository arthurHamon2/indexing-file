from single_threaded_strategy import CSV_TEST
from profiler import Profiler

if __name__ == '__main__':
    # test = CSV_TEST('http://www.lengow.fr/fluxClients/leguide.csv')

    profiler = Profiler()
    for i in range(10):
        with profiler:
            test = CSV_TEST(
               'http://dl-cron.lengow.com/FluxMusikia/VeilleLengow.csv')
            test.execute()
    print(profiler)
