import pickle
from typing import Optional

from job_hive.queue.base import BaseQueue
from job_hive.core import Status
from job_hive.utils import as_string, get_now
from job_hive.job import Job
from typing import Type

try:
    import confluent_kafka
except ImportError:
    raise ImportError('KafkaQueue requires confluent_kafka to be installed.')


class KafkaQueue(BaseQueue):
    def __init__(self, topic_name: str, servers: str, group_id: str= 'custom_consumer'):
        self.topic_name = topic_name
        self.producer = confluent_kafka.Producer({
            'bootstrap.servers': servers,
        })
        self.consumer = confluent_kafka.Consumer({
            'bootstrap.servers': servers,
            'group.id': group_id,
            'enable.auto.commit': 'true',
            'auto.offset.reset': 'smallest',
        })
        self.consumer.subscribe([self.topic_name])

    @property
    def size(self) -> int:
        topics = self.consumer.list_topics(self.topic_name).topics
        if self.topic_name not in topics:
            return 0
        n_msg = 0
        for part in topics[self.topic_name].partitions:
            topic_part = confluent_kafka.TopicPartition(self.topic_name, part)
            start_offset, end_offset = self.consumer.get_watermark_offsets(topic_part)
            n_msg += end_offset - start_offset
        return n_msg

    def enqueue(self, job: 'Job'):
        self.producer.produce(self.topic_name, pickle.dumps(job.dumps()))
        self.producer.flush()

    def remove(self, job: 'Job'):
        pass

    def dequeue(self) -> Optional['Job']:
        msg = self.consumer.poll(2.0)
        if msg is None: return None
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            return None
        else:
            return Job.loads(self._transform_job_mapping(pickle.loads(msg.value())))

    def clear(self):
        self.consumer.close()
        self.producer.close()

    def is_empty(self) -> bool:
        return bool(self.size)
        pass

    def update_status(self, job: 'Job'):
        pass

    def get_job(self, job_id: str) -> Optional['Job']:
        pass

    def ttl(self, job_id: str, ttl: int):
        pass

    def close(self):
        pass

    @staticmethod
    def _transform_job_mapping(job_mapping: dict):
        job_decode_mapping = {}
        for key, value in job_mapping.items():
            key = as_string(key)
            job_decode_mapping[key] = pickle.loads(value) if key in {'args', 'kwargs', 'result',
                                                                     'error'} else as_string(value)
        return job_decode_mapping

    def __repr__(self):
        return f"RedisQueue(name={self.topic_name})"


# producer = confluent_kafka.Producer({
#     'bootstrap.servers': '1Panel-kafka-3wvJ:9092',
# })
# producer.poll(1.0)
# a = KafkaQueue()
# for i in range(1000):
#     a.enqueue(Job(
#         print, f'test{i}'
#     ))
#     print(i)
# print(a.size)
# a.dequeue()
# print(a.size)
