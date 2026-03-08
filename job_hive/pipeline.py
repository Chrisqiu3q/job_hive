from typing import TYPE_CHECKING, Any, Optional, Tuple

if TYPE_CHECKING:
    from job_hive.job import Job


class Pipeline:
    """
    Represents a pipeline of jobs that execute sequentially.
    Each job's output becomes the next job's input.
    """

    def __init__(self, *args: 'Job') -> None:
        self._jobs: Tuple['Job', ...] = tuple(args)
        self._locked = False

    def add_task(self, delay_job: 'Job'):
        """
        Adds a job to the pipeline.
        Can only be called before the pipeline is committed.
        """
        if self._locked:
            raise RuntimeError("Cannot add task to a locked pipeline")
        self._jobs = self._jobs + (delay_job,)

    @property
    def jobs(self) -> Tuple['Job', ...]:
        """Return a read-only tuple of jobs."""
        return self._jobs

    @property
    def is_locked(self) -> bool:
        """Return whether the pipeline is locked (committed)."""
        return self._locked

    def __enter__(self):
        if self._locked:
            raise RuntimeError("Pipeline cannot reuse commit")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            raise RuntimeError("Pipeline commit failed") from exc_val
        self._locked = True

    def __len__(self) -> int:
        return len(self._jobs)

    def __repr__(self) -> str:
        return f"Pipeline(jobs={list(self._jobs)})"

    def execute(self, initial_input: Any = None) -> Any:
        """
        Execute all jobs in the pipeline sequentially.
        The output of each job becomes the input of the next job.
        The first job receives initial_input as its argument.
        Returns the result of the last job.
        """
        if not self._jobs:
            return None

        result = initial_input

        for i, job in enumerate(self._jobs):
            if i == 0:
                # First job uses initial_input if no args provided
                if not job._args and initial_input is not None:
                    result = job(initial_input)
                else:
                    result = job()
            else:
                # Subsequent jobs use previous result as input
                result = job(result)

        return result
