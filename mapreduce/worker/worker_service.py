from enum import Enum, auto
import logging
import threading
import uuid

from mapreduce.proto import worker_pb2
from mapreduce.proto.worker_pb2_grpc import WorkerServiceServicer
from mapreduce.worker.algorithm import STEP_ID_TO_FUNCTION
from .utils import get_file_handles_from_gstorage

class Job:
    class JobStatus(Enum):
        OK = auto()
        WORKING = auto()
        FAILED = auto()

    def __init__(self, stepId, args):
        self.thread = threading.Thread(target=self.wrapper_function, args=(STEP_ID_TO_FUNCTION[stepId], args))
        self.exception_string = None

    def status(self):
        if self.is_working():
            return self.JobStatus.WORKING
        elif self.exception_string is None:
            return self.JobStatus.OK
        else:
            return self.JobStatus.FAILED

    def wrapper_function(self, f, args):
        try:
            f(*args)
        except Exception as e:
            self.exception_string = repr(e)
            raise

    def start(self):
        self.thread.start()

    def is_working(self):
        return self.thread.is_alive()

class WorkerServiceServicerImpl(WorkerServiceServicer):
    """
    Not thread-safe.
    """
    def __init__(self):
        super().__init__()
        self.workerUuid = str(uuid.uuid4())
        self.current_job = None

    def status(self):
        if self.current_job is None:
            return Job.JobStatus.OK
        else:
            return self.current_job.status()

    def is_working(self):
        return self.current_job is not None and self.current_job.is_working()

    def StartStep(self, request, context):
        logging.info("StartStep request:\n" + str(request))

        if self.is_working():
            logging.info("Received step request, but node is still working")
            return worker_pb2.StartStepReply(ok=False, workerUuid=self.workerUuid)

        input_file, output_file = get_file_handles_from_gstorage(
            [request.inputLocation, request.outputLocation]
        )

        self.current_job = Job(request.stepId, (input_file, output_file, request.rangeStart, request.rangeEnd))
        self.current_job.start()
        return worker_pb2.StartStepReply(ok=True, workerUuid=self.workerUuid)

    def WorkerStatus(self, request, context):
        logging.info("WorkerStatus request: " + str(request))

        status = worker_pb2.WorkerStatusReply.WorkerStatusEnum.Ok
        error_msg = None
        match self.status():
            case Job.JobStatus.OK:
                status = worker_pb2.WorkerStatusReply.WorkerStatusEnum.Ok
                self.current_job = None
            case Job.JobStatus.WORKING:
                status = worker_pb2.WorkerStatusReply.WorkerStatusEnum.Working
            case Job.JobStatus.FAILED:
                status = worker_pb2.WorkerStatusReply.WorkerStatusEnum.Failure
                error_msg = self.current_job.exception_string
                self.current_job = None
            case _:
                raise NotImplementedError("Unhandled worker status")

        return worker_pb2.WorkerStatusReply(
            status=status,
            errorMessage=error_msg,
            workerUuid=self.workerUuid
        )
