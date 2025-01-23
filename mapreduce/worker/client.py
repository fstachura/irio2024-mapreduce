import logging
import sys
import grpc
from google.protobuf.empty_pb2 import Empty

from ..proto import worker_pb2
from ..proto import worker_pb2_grpc

def client():
    if len(sys.argv) != 2:
        print("usage: ", sys.argv[0], "host:port")
        return

    with grpc.insecure_channel(sys.argv[1]) as channel:
        stub = worker_pb2_grpc.WorkerServiceStub(channel)

        startStepRequest = worker_pb2.StartStepRequest(
            inputLocation="inputLocationPlaceholder",
            outputLocation="outputLocationPlaceholder",
            stepId="stepIdPlaceholder"
        )
        startStepReply = stub.StartStep(startStepRequest)
        print("StartStep(..):\n" + str(startStepReply))

        nodeStatusReply = stub.WorkerStatus(Empty())
        print("WorkerStatus():\n" + str(nodeStatusReply))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client()
