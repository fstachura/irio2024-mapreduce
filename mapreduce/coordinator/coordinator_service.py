from google.cloud import storage
from sqlalchemy.orm import Session

from ..proto.coordinator_pb2 import LastJobStatusReply, StartJobReply
from ..proto.coordinator_pb2_grpc import CoordinatorServiceServicer, add_CoordinatorServiceServicer_to_server
from .database import Job
from .algorithm import init_algorithm, INIT_STEP
from .utils import get_unfinished_job


class CoordinatorServiceServicerImpl(CoordinatorServiceServicer):
    def __init__(self, db):
        self.db = db

    def StartJob(self, request, context):
        with Session(self.db) as tr:
            storage_client = storage.Client()

            job = Job(input_location=request.inputLocation,
                      output_location=request.outputLocation,
                      current_step=INIT_STEP)
            tr.add(job)
            tr.flush()
            tr.refresh(job)

            init_algorithm(tr, storage_client, job)

            return StartJobReply(jobUuid=str(job.job_uuid))

    def LastJobStatus(self, request, context):
        status = None
        with Session(self.db) as tr:
            job = get_unfinished_job(tr)
            if job is not None:
                job_finished = job.finished is None or job.finished == True
                status = LastJobStatusReply.JobStatus(jobUuid=str(job.job_uuid), finished=job_finished)
        return LastJobStatusReply(status=status)

