import logging
import sys
import grpc

from ..grpc import mapreduce_pb2
from ..grpc import mapreduce_pb2_grpc
from ..grpc.mapreduce_pb2_grpc import NodeAPI, UserAPIServicer

def client():
    if len(sys.argv) != 2:
        print("usage:", sys.argv[0], "host:port")
        return

    with grpc.insecure_channel(sys.argv[1]) as channel:
        stub = mapreduce_pb2_grpc.UserAPIStub(channel)
        result = stub.LastJobStatus(mapreduce_pb2.Empty())
        print("last job uuid:", result.status.jobUuid)
        print("finished:", result.status.finished)

if __name__ == "__main__":
    logging.basicConfig()
    client()

