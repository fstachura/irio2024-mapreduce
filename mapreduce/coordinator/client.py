import logging
import sys
import grpc
from google.protobuf.empty_pb2 import Empty

from ..proto import coordinator_pb2
from ..proto import coordinator_pb2_grpc

def client():
    if len(sys.argv) != 5 and len(sys.argv) != 3:
        print("usage:", sys.argv[0], "submit|status", "host:port", "[bucket:input_dir]", "[bucket:output_file]")
        return

    with grpc.insecure_channel(sys.argv[2]) as channel:
        if sys.argv[1] == "submit":
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            result = stub.StartJob(
                    coordinator_pb2.StartJobRequest(
                        inputLocation=sys.argv[3],
                        outputLocation=sys.argv[4]
                    )
                )
            print("job id:", result)
        elif sys.argv[1] == "status":
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            result = stub.LastJobStatus(Empty())
            print(result)
        else:
            print("invalid command")

if __name__ == "__main__":
    logging.basicConfig()
    client()

