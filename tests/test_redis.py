from job_hive.queue.redis_queue import RedisQueue
from job_hive.work import HiveWork

work = HiveWork(queue=RedisQueue(name="test", host="192.168.6.99", password="redis-test"))


@work.task()
def hello(index):
    print('你是', index)


if __name__ == '__main__':
    hello(1)

    work.work()
