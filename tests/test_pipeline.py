"""Test pipeline functionality."""
import unittest
import sys
import os
import uuid
import pickle
import inspect
from enum import StrEnum
from typing import Optional, Any


# Copy necessary classes from the project to avoid redis dependency
class Status(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"


def get_now():
    from datetime import datetime
    return datetime.now().isoformat()


def import_attribute(name):
    """Import an attribute from a module."""
    module_name, attribute = name.rsplit('.', 1)
    module = __import__(module_name, fromlist=[attribute])
    return getattr(module, attribute)


class Job:
    def __init__(self, func, *args, **kwargs):
        self.job_id = str(uuid.uuid4())
        self.func = self._get_func_path(func)
        self._args = args
        self.query = {
            "status": Status.PENDING.value,
            "created_at": get_now(),
        }
        self._kwargs = kwargs

    @staticmethod
    def _get_func_path(func):
        if inspect.isfunction(func) or inspect.isbuiltin(func):
            return '{0}.{1}'.format(func.__module__, func.__qualname__)
        elif isinstance(func, str):
            return func
        else:
            raise TypeError('Expected a callable or a string, but got: {0}'.format(func))

    @property
    def created_at(self) -> Optional[str]:
        return self.query.get("created_at", '')

    @property
    def ended_at(self) -> Optional[str]:
        return self.query.get("ended_at", '')

    @property
    def started_at(self) -> Optional[str]:
        return self.query.get("started_at", '')

    @property
    def status(self) -> Optional[Status]:
        return Status(self.query.get("status", Status.PENDING.value))

    @property
    def result(self) -> Any:
        return self.query.get("result", None)

    @property
    def error(self) -> Any:
        return self.query.get("error", None)

    def __call__(self, *args, **kwargs):
        func = import_attribute(self.func)
        if hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        # If args are provided, use them; otherwise use self._args
        if args:
            return func(*args, **self._kwargs)
        return func(*self._args, **self._kwargs)

    def __repr__(self):
        return f"[Job {self.job_id}]"


class Pipeline:
    """
    Represents a pipeline of jobs that execute sequentially.
    Each job's output becomes the next job's input.
    """

    def __init__(self, *args: Job) -> None:
        self._jobs = list(args)
        self._locked = False

    def add_task(self, delay_job: Job):
        """
        Adds a job to the pipeline.
        """
        self._jobs.append(delay_job)

    @property
    def jobs(self):
        return self._jobs

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


# Test functions
def add_one(x):
    return x + 1


def multiply_by_two(x):
    return x * 2


def to_string(x):
    return str(x)


class TestPipeline(unittest.TestCase):

    def test_pipeline_init(self):
        """Test pipeline initialization."""
        job1 = Job(add_one, 1)
        job2 = Job(multiply_by_two, 2)
        pipeline = Pipeline(job1, job2)

        self.assertEqual(len(pipeline), 2)
        self.assertEqual(len(pipeline.jobs), 2)

    def test_pipeline_add_task(self):
        """Test adding tasks to pipeline."""
        job1 = Job(add_one, 1)
        pipeline = Pipeline(job1)

        job2 = Job(multiply_by_two, 2)
        pipeline.add_task(job2)

        self.assertEqual(len(pipeline), 2)

    def test_pipeline_context_manager(self):
        """Test pipeline context manager locks the pipeline."""
        job1 = Job(add_one, 1)
        pipeline = Pipeline(job1)

        with pipeline:
            pass

        # After context manager, pipeline should be locked
        self.assertTrue(pipeline.lock)

        # Trying to use context manager again should raise RuntimeError
        with self.assertRaises(RuntimeError):
            with pipeline:
                pass

    def test_pipeline_execute(self):
        """Test pipeline execution with sequential jobs."""
        # Create jobs - first job takes initial input
        job1 = Job(add_one)  # Will receive initial input
        job2 = Job(multiply_by_two)  # Will receive job1's output
        job3 = Job(to_string)  # Will receive job2's output

        pipeline = Pipeline(job1, job2, job3)

        # Execute pipeline with initial input 5
        # Step 1: add_one(5) = 6
        # Step 2: multiply_by_two(6) = 12
        # Step 3: to_string(12) = "12"
        result = pipeline.execute(5)

        self.assertEqual(result, "12")

    def test_pipeline_execute_with_first_job_args(self):
        """Test pipeline when first job has its own args."""
        job1 = Job(add_one, 10)  # Has its own args
        job2 = Job(multiply_by_two)  # Uses previous result

        pipeline = Pipeline(job1, job2)

        # Execute pipeline - first job uses its own args (10)
        # Step 1: add_one(10) = 11
        # Step 2: multiply_by_two(11) = 22
        result = pipeline.execute()

        self.assertEqual(result, 22)

    def test_pipeline_empty(self):
        """Test empty pipeline returns None."""
        pipeline = Pipeline()
        result = pipeline.execute(5)
        self.assertIsNone(result)

    def test_pipeline_single_job(self):
        """Test pipeline with single job."""
        job1 = Job(add_one)
        pipeline = Pipeline(job1)

        result = pipeline.execute(5)
        self.assertEqual(result, 6)

    def test_pipeline_repr(self):
        """Test pipeline string representation."""
        job1 = Job(add_one, 1)
        job2 = Job(multiply_by_two, 2)
        pipeline = Pipeline(job1, job2)

        repr_str = repr(pipeline)
        self.assertIn("Pipeline", repr_str)
        self.assertIn("jobs", repr_str)


if __name__ == '__main__':
    unittest.main()
