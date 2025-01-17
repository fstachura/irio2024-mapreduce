from google.cloud import storage
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..proto.coordinator_pb2 import LastJobStatusReply, StartJobReply
from ..proto.coordinator_pb2_grpc import CoordinatorServiceServicer, add_CoordinatorServiceServicer_to_server
from .database import Job, JobPart
from .algorithm import init_algorithm, INIT_STEP


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
            stmt = select(Job).order_by(desc(Job.id))
            job = tr.execute(stmt).scalars().first()

            if job is not None:
                job_finished = job.finished is None or job.finished == True

                stmt = select(JobPart).where(JobPart.job_id == job.id, JobPart.step == job.current_step)
                parts = tr.execute(stmt).scalars().all()
                parts_finished = [part for part in parts if part.finished]

                status = LastJobStatusReply.JobStatus(
                        jobUuid=str(job.job_uuid),
                        finished=job_finished,
                        currentStep=job.current_step,
                        stepPartsFinished=len(parts_finished),
                        stepPartsTotal=len(parts),
                        inputLocation=job.input_location,
                        outputLocation=job.output_location)
        return LastJobStatusReply(status=status)

