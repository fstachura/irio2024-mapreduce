import logging
import sys
import requests
import grpc
from google.protobuf.empty_pb2 import Empty

from ..proto import coordinator_pb2
from ..proto import coordinator_pb2_grpc

def client():
    if len(sys.argv) <= 2:
        print("usage:", sys.argv[0], "upload|submit|status", "host:port", "...")
        return

    with grpc.insecure_channel(sys.argv[2]) as channel:
        if sys.argv[1] == "submit":
            if len(sys.argv) != 5:
                print("usage:", sys.argv[0], "submit", "host:port", "[bucket:input_dir]", "[bucket:output_file]")
                return

            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            result = stub.StartJob(
                    coordinator_pb2.StartJobRequest(
                        inputLocation=sys.argv[3],
                        outputLocation=sys.argv[4]
                    )
                )
            print("job id:", result)
        elif sys.argv[1] == "status":
            if len(sys.argv) != 3:
                print("usage:", sys.argv[0], "status", "host:port")
                return

            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            result = stub.LastJobStatus(Empty())
            print(result)
        elif sys.argv[1] == "upload":
            if len(sys.argv) <= 3:
                print("usage:", sys.argv[0], "upload", "host:port", "filename...")
                return

            filenames = sys.argv[3:]
            files = [open(f) for f in filenames]
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            result = stub.UploadFiles(
                        coordinator_pb2.UploadFilesRequest(numberOfFiles=len(files))
                    )

            print("Got upload URLs")
            print("Input location:", result.inputLocation)
 
            for (f, filename, url) in zip(files, filenames, result.uploadUrls):
                print("Uploading", filename)
                result = requests.put(url, f, headers={'Content-Type': 'application/octet-stream'})
                if result.status_code != 200:
                    print("Failed to upload file", result.status_code, result.text)
                    break

        else:
            print("invalid command")

if __name__ == "__main__":
    logging.basicConfig()
    client()

