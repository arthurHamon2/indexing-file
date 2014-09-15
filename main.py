from consume_file import Runner

if __name__ == '__main__':
    runner = Runner(nb_consumer=3)
    runner.start()
