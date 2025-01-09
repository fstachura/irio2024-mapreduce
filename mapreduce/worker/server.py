from concurrent import futures
import logging
import os
import grpc
from ..grpc import mapreduce_pb2
from ..grpc import mapreduce_pb2_grpc
from ..grpc.mapreduce_pb2_grpc import NodeAPIServicer
from google.cloud import storage

class NodeAPIServicerImpl(NodeAPIServicer):
    def StartStep(self, request, context):
        logging.info("StartStep request:\n" + str(request))

        storage_client = storage.Client()

        input_bucket_name, input_file_name = request.inputLocation.split(':')
        output_bucket_name, output_file_name = request.outputLocation.split(':')

        input_bucket = storage_client.bucket(input_bucket_name)
        input_file = input_bucket.blob(input_file_name)

        output_bucket = storage_client.bucket(output_bucket_name)
        output_file = output_bucket.blob(output_file_name)

        with input_file.open("r") as r, output_file.open("w") as w:
            w.write(str(request))
            w.write(r.read())

        return mapreduce_pb2.StartStepReply(ok=True)
    
    def NodeStatus(self, request, context):
        logging.info("NodeStatus request: " + str(request))
        status = mapreduce_pb2.NodeStatusReply.NodeStatusEnum.Ok
        return mapreduce_pb2.NodeStatusReply(status=status)

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
    logging.basicConfig()
    serve()