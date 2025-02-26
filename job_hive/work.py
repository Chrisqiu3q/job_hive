import time
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
from typing import TYPE_CHECKING, Optional

from job_hive.core import Status
from job_hive.job import Job
from job_hive.logger import LiveLogger
from job_hive.utils import get_now

if TYPE_CHECKING:
    from job_hive.queue.base import BaseQueue


class HiveWork:
    def __init__(self, queue: 'BaseQueue'):
        self.logger: Optional[LiveLogger] = None
        self._queue = queue
        self._process_pool: Optional[ProcessPoolExecutor] = None

    def push(self, func, *args, **kwargs) -> 'Job':
        job = Job(func, *args, **kwargs)
        self._queue.enqueue(job)
        return job

    def pop(self) -> Optional['Job']:
        return self._queue.dequeue()

    def work(self, prefetching: int = 1, waiting: int = 3, concurrent: int = 1):
        self.logger = LiveLogger()
        self.logger.info(r"""
        
   $$$$$\  $$$$$$\  $$$$$$$\          $$\   $$\ $$$$$$\ $$\    $$\ $$$$$$$$\ 
   \__$$ |$$  __$$\ $$  __$$\         $$ |  $$ |\_$$  _|$$ |   $$ |$$  _____|
      $$ |$$ /  $$ |$$ |  $$ |        $$ |  $$ |  $$ |  $$ |   $$ |$$ |      
      $$ |$$ |  $$ |$$$$$$$\ |$$$$$$\ $$$$$$$$ |  $$ |  \$$\  $$  |$$$$$\    
$$\   $$ |$$ |  $$ |$$  __$$\ \______|$$  __$$ |  $$ |   \$$\$$  / $$  __|   
$$ |  $$ |$$ |  $$ |$$ |  $$ |        $$ |  $$ |  $$ |    \$$$  /  $$ |      
\$$$$$$  | $$$$$$  |$$$$$$$  |        $$ |  $$ |$$$$$$\    \$  /   $$$$$$$$\ 
 \______/  \______/ \_______/         \__|  \__|\______|    \_/    \________|

prefetching: {}
waiting: {}
concurrent: {}
Started work...
""".format(prefetching, waiting, concurrent))
        self._process_pool = ProcessPoolExecutor(max_workers=concurrent)
        run_jobs = {}
        while True:
            if len(run_jobs) >= prefetching:
                flush_jobs = {}
                for job_id, (future, job) in run_jobs.items():
                    if not future.done():
                        flush_jobs[job_id] = (future, job)
                        continue
                    job.query["ended_at"] = get_now()
                    try:
                        job.query["result"] = str(future.result())
                        job.query["status"] = Status.SUCCESS.value
                        self.logger.info(f"Successes job: {job.job_id}")
                        self._queue.ttl(job.job_id, 24 * 60 * 60)
                    except Exception as e:
                        job.query["error"] = str(e)
                        job.query["status"] = Status.FAILURE.value
                        self.logger.error(f"Failures job: {job.job_id}")
                    finally:
                        self._queue.update_status(job)
                run_jobs = flush_jobs
            else:
                job = self.pop()
                if job is None:
                    time.sleep(waiting)
                    continue
                self.logger.info(f"Started job: {job.job_id}")
                future = self._process_pool.submit(job)
                run_jobs[job.job_id] = (future, job)
                self._queue.update_status(job)

    def get_job(self, job_id: str) -> Optional['Job']:
        return self._queue.get_job(job_id)

    def task(self):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs) -> 'Job':
                return self.push(func, *args, **kwargs)

            return wrapper

        return decorator

    def __len__(self) -> int:
        return self._queue.size

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._process_pool is None:
            return
        self._process_pool.shutdown()

    def __enter__(self):
        return self

    def __del__(self):
        if self._process_pool is None:
            return
        self._process_pool.shutdown()

    def __repr__(self):
        return f"<HiveWork queue={self._queue}>"
