from src.cacheout.lru import LRUCache
import threading
import queue
import time
import random

q = queue.Queue()
cache = LRUCache(maxsize=3, q=q)


def que():
    while 1:
        print(q.get(block=True))


def cac():
    while 1:
        cache.set(random.randint(1000, 9999), random.randint(1000, 9999))
        print(cache.items())
        time.sleep(2)


t1 = threading.Thread(target=que)
t2 = threading.Thread(target=cac)
t1.start()
t2.start()

t1.join()
t2.join()
