import time
import sys
import re

from dask.distributed import Client
from dask import delayed

FILES = ["words/part-00000", "words/part-00001", "words/part-00002", "words/part-00003"]


def countwords(f):
    space = re.compile("\W+")
    counter = 0
    with open(f, "r") as infile:
        for line in infile:
            tokens = space.split(line.strip())
            counter = counter + len(tokens)
    return counter


def countlines(f):
    counter = 0
    with open(f, "r") as infile:
        for line in infile:
            counter = counter + 1
    return counter


def serial(fun):
    """ applies the function fun to each part file in order
    e.g., serial(countwords), serial(countlines)
    """
    counter = 0
    start = time.perf_counter()
    for f in FILES:
        counter = counter + fun(f)
    end = time.perf_counter()
    print(f"found {counter} lines in time {end-start}")


def parallel(fun):
    """ applies the function fun in parallel """
    c = Client(n_workers=4)
    # print(f"dashboard: {c.cluster.dashboard_link}")
    #_ = input("started cluster. press any key to continue") # pause
    setup_start = time.perf_counter()
    objects = []
    for f in FILES:
        objects.append(delayed(fun)(f))
    z = delayed(sum)(objects)
    setup_end = time.perf_counter()
    start = time.perf_counter()
    counter = z.compute()
    end = time.perf_counter()
    print(f"found {counter} lines in time {end-start} = {setup_end-setup_start} for startup, {end-start} for execution")
    #_ = input("finished. press any key to continue") # pause
    c.close()

#
# If your workers have a lot to do (each job is not trivial), then it makes sense to do parallel programming
#

