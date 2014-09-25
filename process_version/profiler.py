import time
import multiprocessing


class Profiler:
    """
    Simple profiler provided as a context manager.
    """
    def __init__(self):
        self.start_time = None
        self.execution_times = []
        self.average = 0
        self.min = 0
        self.max = 0

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, type, value, traceback):
        exec_time = time.time() - self.start_time
        self.execution_times.append(exec_time)
        print(exec_time, " seconds - ", multiprocessing.current_process().name)
        self.start_time = None

    def __str__(self):
        for ex in self.execution_times:
            self.average += ex
        self.average = self.average / len(self.execution_times)
        self.min = min(self.execution_times)
        self.max = max(self.execution_times)
        string = 'execution report:\n'
        string += 'average {}\n'.format(self.average)
        string += 'min {}\n'.format(self.min)
        string += 'max {}\n'.format(self.max)
        return string
