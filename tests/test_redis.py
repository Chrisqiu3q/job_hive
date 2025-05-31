from job_hive.queue import RedisQueue
from job_hive import HiveWork

work = HiveWork(queue=RedisQueue(name="test"))


@work.task()
def hello(index):
    print('你是', index)
    raise Exception('test')


if __name__ == '__main__':
    work.work(result_ttl=30)