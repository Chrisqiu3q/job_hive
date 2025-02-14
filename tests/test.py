from task_hive.work import HiveWork
from task_hive.queue.redis_queue import RedisQueue

if __name__ == '__main__':

    with HiveWork(queue=RedisQueue(name="test", host="192.168.6.99", password="redis-test")) as work:
        jobs = []
        for i in range(1):
            jobs.append(work.push(print, f"hello {i}"))
        work.work()
