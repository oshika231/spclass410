""" For more cools stuff, check out the dask.delayed API https://docs.dask.org/en/latest/delayed-api.html"""

import time
import sys

from dask.distributed import Client
from dask import delayed




def increment(x, sleeptime=1):
    """ returns 1+x """
    print(f"I am about to increment {x}")
    time.sleep(sleeptime)
    return x+1


def add(x, y, sleeptime=1):
    """ returns x+y """
    print(f"Going to add {x} and {y}")
    time.sleep(sleeptime)
    return x+y


def double(x, sleeptime=1):
    """ returns 2x 
    note that the @delayed decorator means that calling this function
    actually returns a delayed object
    """
    print(f"I am about to double {x}")
    time.sleep(sleeptime)
    return 2*x

@delayed
def increment2(x, sleeptime=1):
    """ returns 1+x, is equivalent to delayed(increment)(x) """
    print(f"I am about to increment {x}")
    time.sleep(sleeptime)
    return x+1

@delayed
def add2(x, y, sleeptime=1):
    """ returns x+y """
    print(f"Going to add {x} and {y}")
    time.sleep(sleeptime)
    return x+y

@delayed
def double2(x, sleeptime=1):
    """ returns 2x 
    note that the @delayed decorator means that calling this function
    actually returns a delayed object
    """
    print(f"I am about to double {x}")
    time.sleep(sleeptime)
    return 2*x


def serial():
    start = time.perf_counter()
    x = increment(1)
    y = increment(2)
    z = add(x,y)
    end = time.perf_counter()
    print(f"The result is {z}")
    print(f"Computation time is {end-start}")
    return z


def parallel():
    c = Client(n_workers=4)
    # set up the computation graph
    setup_start = time.perf_counter()
    x = delayed(increment)(1)
    y = delayed(increment)(2)
    z = delayed(add)(x,y)
    setup_end = time.perf_counter()
    print(f"Setup time is {setup_end-setup_start}")
    z.visualize('z-visualize.png')

    # run the computation graph
    start = time.perf_counter()
    result = z.compute()
    end = time.perf_counter()
    print(f"The result is {result}")
    print(f"Computation time is {end-start}")
    c.close()

# serial version:
# sum([increment(i) for i in range(n)])

def loopy(n, version=0):
    c = Client(n_workers=4)
    operations = [delayed(increment)(i) for i in range(n)] # 
    if version == 0:
        z = delayed(sum)(operations)
    else:
        z = sum(operations)
    z.visualize(f"loopy.png-version-{version}")

    # run the computation graph
    start = time.perf_counter()
    result = z.compute()
    end = time.perf_counter()
    print(f"The result is {result}")
    print(f"Computation time is {end-start}")

    c.close()
    
    
def parallel_original():
    c = Client(n_workers=4)
    # set up the computation graph
    x = delayed(increment)(1)
    y = delayed(increment)(2)
    z = delayed(add)(x,y)
    result = z.compute()
    return z

def parallel_using_decorated_functions():
    c = Client(n_workers=4)
    # set up the computation graph
    x = increment2(1) # x is a delayed object
    y = increment2(2) # y is still a delayed object
    z = add2(x,y)  # z is a delayed object
    result = z.compute()
    return z


def control(n):
    c = Client(n_workers=4)
    #operations = [delayed(increment)(i) if i % 2 == 0 else delayed(double)(i)  for i in range(n)]
    operations = [increment2(i) if i % 2 == 0 else double2(i)  for i in range(n)]
    z = delayed(sum)(operations)
    z.visualize("control.png")
    result = z.compute()
    print(f"The result is {result}")
