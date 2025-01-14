import logging
import sys
import grpc
from google.protobuf.empty_pb2 import Empty

from ..proto import coordinator_pb2
from ..proto import coordinator_pb2_grpc

def client():
    if len(sys.argv) != 2:
        print("usage:", sys.argv[0], "host:port")
        return

    with grpc.insecure_channel(sys.argv[1]) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        result = stub.LastJobStatus(Empty())
        print("last job uuid:", result.status.jobUuid)
        print("finished:", result.status.finished)

if __name__ == "__main__":
    logging.basicConfig()
    client()

