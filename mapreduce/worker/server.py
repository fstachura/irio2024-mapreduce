from concurrent import futures
from enum import Enum, auto
import threading
import logging
import os
import grpc
import uuid
from google.cloud import storage
from google.protobuf.empty_pb2 import Empty
from ..proto import worker_pb2
from ..proto import worker_pb2_grpc
from ..proto.worker_pb2_grpc import WorkerServiceServicer

# Location is of the form "bucket_name:file_path".
def get_file_handles_from_gstorage(locationsList):
    storage_client = storage.Client()

    file_handles = []
    for location in locationsList:
        bucket_name, file_path = location.split(':')
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)

        file_handles.append(blob)

    return file_handles


class Job:
    class JobStatus(Enum):
        OK = auto()
        WORKING = auto()
        FAILED = auto()

    def __init__(self, target, args):
        self.thread = threading.Thread(target=target, args=(self, *args))
        self.exception_string = None

    def status(self):
        if self.is_working():
            return self.JobStatus.WORKING
        elif self.exception_string is None:
            return self.JobStatus.OK
        else:
            return self.JobStatus.FAILED

    def start(self):
        self.thread.start()

    def is_working(self):
        return self.thread.is_alive()

    def map_function(self, input_file, output_file):
        try:
            with input_file.open('r') as r, output_file.open('w') as w:
                for line in r:
                    for word in line.split():
                        w.write(f"{word},1\n")
        except Exception as e:
            self.exception_string = repr(e)
            raise

    def reduce_function(self, input_file, output_file):
        try:
            word_cnt = {}
            with input_file.open('r') as r:
                for line in r:
                    word, cnt = line.split(',')
                    word_cnt[word] = word_cnt.get(word, 0) + int(cnt)

            with output_file.open('w') as w:
                for word, cnt in word_cnt.items():
                    w.write(f"{word},{cnt}\n")
        except Exception as e:
            self.exception_string = repr(e)
            raise

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

        match request.stepId:
            case "map":
                self.current_job = Job(Job.map_function, (input_file, output_file))
                self.current_job.start()
                return worker_pb2.StartStepReply(ok=True, workerUuid=self.workerUuid)
            case "reduce":
                self.current_job = Job(Job.reduce_function, (input_file, output_file))
                self.current_job.start()
                return worker_pb2.StartStepReply(ok=True, workerUuid=self.workerUuid)
            case stepId:
                logging.error(f"StartStep() unimplemented for provided StepId: {stepId}")
                return worker_pb2.StartStepReply(ok=False, workerUuid=self.workerUuid)
    
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

        return worker_pb2.NodeStatusReply(
            status=status,
            errorMessage=error_msg,
            workerUuid=self.workerUuid
        )

def serve():
    # Worker node processes one request at a time.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(
    )
    port = os.environ.get("HTTP_PORT", "[::]:50051")
    server.add_insecure_port(port)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
