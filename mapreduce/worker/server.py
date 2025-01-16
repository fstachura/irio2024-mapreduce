from concurrent import futures
import threading
import logging
import os
import grpc
import uuid
from google.cloud import storage
from ..grpc import mapreduce_pb2
from ..grpc import mapreduce_pb2_grpc
from ..grpc.mapreduce_pb2_grpc import NodeAPIServicer

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

def map_function(input_file, output_file):
    with input_file.open('r') as r, output_file.open('w') as w:
        for line in r:
            for word in line.split():
                w.write(f"{word},1\n")

def reduce_function(input_file, output_file):
    with input_file.open('r') as r, output_file.open('w') as w:
        line_cnt = 0
        for line in r:
            word, cnt = line.split(',')
            line_cnt += int(cnt)
        w.write(f"{word},{line_cnt}")

class NodeAPIServicerImpl(NodeAPIServicer):
    def __init__(self):
        super().__init__()
        self.workerUuid = str(uuid.uuid4())
        self.current_job = None
    
    def is_working(self):
        if self.current_job is not None:
            if self.current_job.is_alive():
                return True
            else:
                self.current_job = None
                return False
        else:
            return False

    def StartStep(self, request, context):
        logging.info("StartStep request:\n" + str(request))

        if self.is_working():
            logging.info("Received step request, but node is still working")
            return mapreduce_pb2.StartStepReply(ok=False, workerUuid=self.workerUuid)

        input_file, output_file = get_file_handles_from_gstorage(
            [request.inputLocation, request.outputLocation]
        )

        match request.stepId:
            case "map":
                self.current_job = threading.Thread(target=map_function, args=(input_file, output_file))
                self.current_job.start()
                return mapreduce_pb2.StartStepReply(ok=True, workerUuid=self.workerUuid)
            case "reduce":
                self.current_job = threading.Thread(target=reduce_function, args=(input_file, output_file))
                self.current_job.start()
                return mapreduce_pb2.StartStepReply(ok=True, workerUuid=self.workerUuid)
            case stepId:
                logging.error(f"StartStep() unimplemented for provided StepId: {stepId}")
                return mapreduce_pb2.StartStepReply(ok=False, workerUuid=self.workerUuid)
    
    def NodeStatus(self, request, context):
        logging.info("NodeStatus request: " + str(request))
        
        status = mapreduce_pb2.NodeStatusReply.NodeStatusEnum.Unspecified
        if self.is_working():
            status = mapreduce_pb2.NodeStatusReply.NodeStatusEnum.Working
        else:
            status = mapreduce_pb2.NodeStatusReply.NodeStatusEnum.Ok

        return mapreduce_pb2.NodeStatusReply(status=status, workerUuid=self.workerUuid)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_NodeAPIServicer_to_server(
        NodeAPIServicerImpl(), server
    )
    port = os.environ.get("HTTP_PORT", "[::]:50051")
    server.add_insecure_port(port)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()