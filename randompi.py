from dask.distributed import Client
from dask import delayed
import time
import random
import sys


@delayed
def estimate():
    # inside of the for loop 
    x = random.uniform(-1, 1) # random x coordinate in a 2x2 square
    y = random.uniform(-1, 1) # random y coordinate in a 2x2 square
    # a circle with radius 1 satisfies x^2 + y^2 <=1
    # area of square: 4
    # area of circle: pi
    # what fraction of the points in the square are in the circle pi/4
    if x*x + y*y <= 1: 
        result = 1
    else:
        result = 0
    return result


@delayed
def estimate_batch(size):
    result = 0
    # to speed up the parallelism, we give each worker more stuff to do. Instead of 1 simulation
    # each worker will do `size' simulations
    for _ in range(size):
        x = random.uniform(-1, 1)
        y = random.uniform(-1, 1)
        if x*x + y*y <= 1:
            result = result + 1
    return result
    #this is kind of like the serial version, except instead of doing the serial version
    # with 1_000_000 loop iterations, we are going to give each worker 1_000_000/num_workers iterations


def pi1(num): # num is number of points in a 2x2 square we generate
    # serial monte carlo estimation of pi
    tot  = 0 # will be number of points in the square that are also in the circle
    start = time.perf_counter()
    for _ in range(num):
        x = random.uniform(-1, 1) # random x coordinate in a 2x2 square
        y = random.uniform(-1, 1) # random y coordinate in a 2x2 square
        if x*x + y*y <= 1:
            tot = tot + 1
    # tot/num is rouhgly the probability that a random square point is in the circle
    # we know the the exact probability that a random square point is in the circle is pi/4
    # so tot/num is approximately pi/4
    pi_estimate = 4*tot/float(num)
    end = time.perf_counter()
    print(f"pi1: {pi_estimate} in time {end-start}")


def pi2(num): # our first parallel version
    with Client(n_workers=4) as c: # as soon as we are done with the 'with' statement, it will auto close c
        start = time.perf_counter()
        sims = [estimate() for _ in range(num)] # create a bunch of delayed objects
        tot_delayed = delayed(sum)(sims)
        tot = tot_delayed.compute()
        pi_estimate = 4*tot/float(num)
        end = time.perf_counter()
        print(f"pi2 (parallel): {pi_estimate} in time {end-start}")
        # this ended up being way too slow because if we call pi2(1_000_000) then there are 1 million
        # tasks to do, and this means that we have to manage 1 million tasks and this management takes
        # longer than what we save from parallelism because each task is so fast, so we spend more time
        # managing than computing


def pi3(num):
    N_WORKERS = 4
    with Client(n_workers=N_WORKERS) as c: # as soon as we are done with the 'with' statement, it will auto close c
        start = time.perf_counter()
        
        sims = [estimate_batch(int(num/N_WORKERS)) for _ in range(N_WORKERS)] # create a bunch of delayed objects
        # now each worker does a loop of num/n_workers. So if we wanted to do 1 million simulations with 4
        # workers, each worker does 250,000 simulations and then combine the results
        tot_delayed = delayed(sum)(sims)

        tot = tot_delayed.compute()
        pi_estimate = 4*tot/float(num)
        end = time.perf_counter()
        print(f"pi3 (parallel): {pi_estimate} in time {end-start}")
