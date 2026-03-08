import uuid
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from job_hive import Job


class Pipeline:
    """
    Represents a pipeline of jobs in the job hive.
    Jobs are executed in sequence, with each job's result passed to the next.
    """

    def __init__(self, *args: 'Job') -> None:
        self._jobs = list(args)
        self._locked = False
        self.pipeline_id: str = str(uuid.uuid4())
        self._setup_pipeline_relationships()

    def add_task(self, delay_job: 'Job') -> None:
        """
        Adds a job to the pipeline.
        Optimized: only updates dependencies for the new job and the previous last job,
        avoiding a full traversal of all jobs.
        """
        if self._locked:
            raise RuntimeError("Cannot add task to locked pipeline")
        
        last_job = self.last_job
        self._jobs.append(delay_job)
        
        delay_job.pipeline_id = self.pipeline_id
        
        if last_job:
            last_job.pipeline_next_job_id = delay_job.job_id
            delay_job.pipeline_prev_job_id = last_job.job_id

    def _setup_pipeline_relationships(self):
        """
        Set up the relationships between jobs in the pipeline.
        """
        for i, job in enumerate(self._jobs):
            job.pipeline_id = self.pipeline_id
            if i > 0:
                job.pipeline_prev_job_id = self._jobs[i - 1].job_id
            if i < len(self._jobs) - 1:
                job.pipeline_next_job_id = self._jobs[i + 1].job_id

    @property
    def jobs(self):
        return self._jobs

    @property
    def first_job(self) -> Optional['Job']:
        return self._jobs[0] if self._jobs else None

    @property
    def last_job(self) -> Optional['Job']:
        return self._jobs[-1] if self._jobs else None

    @property
    def lock(self):
        return self._locked

    def __enter__(self):
        if self._locked:
            raise RuntimeError("Pipeline cannot reuse commit")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            raise RuntimeError("Pipeline commit failed") from exc_val
        self._locked = True

    def __len__(self):
        return len(self._jobs)

    def __repr__(self):
        return f"Pipeline(jobs={self._jobs})"
