from uuid import uuid4
from google.cloud import storage
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..proto.coordinator_pb2 import LastJobStatusReply, StartJobReply, UploadFilesReply
from ..proto.coordinator_pb2_grpc import CoordinatorServiceServicer, add_CoordinatorServiceServicer_to_server
from .database import Job, JobPart
from .algorithm import init_algorithm, INIT_STEP
from .utils import generate_upload_url


class CoordinatorServiceServicerImpl(CoordinatorServiceServicer):
    def __init__(self, db, bucket_name, expected_parts):
        self.db = db
        self.bucket_name = bucket_name
        self.expected_parts = expected_parts

    def StartJob(self, request, context):
        with Session(self.db) as tr:
            storage_client = storage.Client()

            job = Job(input_location=request.inputLocation,
                      output_location=request.outputLocation,
                      current_step=INIT_STEP,
                      expected_parts=max(self.expected_parts.get(), 1))
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

    def UploadFiles(self, request, context):
        dir_uuid = uuid4()
        filenames = [f"{dir_uuid}/{uuid4()}" for _ in range(request.numberOfFiles)]
        urls = [generate_upload_url(self.bucket_name, f) for f in filenames]
        return UploadFilesReply(uploadUrls=urls, inputLocation=f"{self.bucket_name}:{dir_uuid}")

