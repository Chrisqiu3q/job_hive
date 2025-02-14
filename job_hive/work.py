import time
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

from job_hive.core import Status
from job_hive.job import Job
from job_hive.utils import get_now

if TYPE_CHECKING:
    from job_hive.base import BaseQueue


class HiveWork:
    def __init__(self, queue: 'BaseQueue'):
        self._queue = queue
        self._process_pool: Optional[ProcessPoolExecutor] = None

    def push(self, func, *args, **kwargs):
        job = Job(func, *args, **kwargs)
        self._queue.enqueue(job)
        return job

    def pop(self) -> Optional['Job']:
        return self._queue.dequeue()

    def work(self, prefetching: int = 1, waiting: int = 3, concurrent: int = 1):
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
                    except Exception as e:
                        job.query["error"] = str(e)
                        job.query["status"] = Status.FAILURE.value
                    finally:
                        self._queue.update_status(job)
                run_jobs = flush_jobs

            job = self.pop()
            if job is None:
                time.sleep(waiting)
                continue
            future = self._process_pool.submit(job)
            run_jobs[job.job_id] = (future, job)
            self._queue.update_status(job)

    def get_job(self, job_id: str) -> Optional['Job']:
        return self._queue.get_job(job_id)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._process_pool is None:
            return
        self._process_pool.shutdown()

    def __enter__(self):
        return self
