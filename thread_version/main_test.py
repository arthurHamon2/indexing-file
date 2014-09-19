from profiler import Profiler
import threading
from multiprocessing import Process, Value, Lock

class MyProcess(Process):

    def __init__(self, gen):
        super().__init__()
        self.generator = gen

    def run(self):
        for i in gen:
            print(i)

def countDown(n):
    while n > 0:
        n -= 1
    print(n)

def generator():
    for i in range(5):
        yield i

class Generator:
    """Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    """
    def __init__(self, it):
        self.it = it
        self.lock = Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.it)

class TestGen:

    def __init__(self, generator):
        super().__init__()
        self.val = Value('i', 0)
        self.generator = generator
        self.lock = Lock()

    def getValue(self):
        with self.lock:
            try:
                print(self.generator)
                self.val.value = next(self.generator)
            except StopIteration:
                #import pdb; pdb.set_trace()
                return None
            return self.val.value

COUNT = 50000000



def gen_proc(generator):
    print(generator)
    i = 0
    while i != None:
        i = generator.getValue()
        print("{}".format(i))

# for i in gen:
#     print(i)

# profiler = Profiler()
# with profiler:
#     countDown(COUNT)
# #print(profiler)

# profiler = Profiler()
# t1 = Thread(target=countDown, args=(COUNT//2,))
# t2 = Thread(target=countDown, args=(COUNT//2,))
# with profiler:
#     t1.start()
#     t2.start()
#     t1.join()
#     t2.join()

gen = generator()
gen = TestGen(gen)

profiler = Profiler()
t1 = Process(target=gen_proc, args=(gen,))
t2 = Process(target=gen_proc, args=(gen,))
with profiler:
    t1.start()
    t2.start()
    t1.join()
    t2.join()

# profiler = Profiler()
# gen = generator()
# t1 = MyProcess(gen)
# t2 = MyProcess(gen)
# with profiler:
#     t1.start()
#     t2.start()
#     t1.join()
#     t2.join()
