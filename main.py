from consume_file import ContextRunner
from strategy_file import TEST

if __name__ == '__main__':
    strategy = TEST('url_test')
    runner = ContextRunner(strategy)
    runner.start()
