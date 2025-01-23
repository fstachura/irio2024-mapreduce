all: mapreduce/proto/coordinator_pb2.py mapreduce/proto/worker_pb2.py

mapreduce/proto/coordinator_pb2.py: mapreduce/proto/coordinator.proto
	python3 -m grpc_tools.protoc -Imapreduce/proto=mapreduce/proto --python_out=. --grpc_python_out=. $^

mapreduce/proto/worker_pb2.py: mapreduce/proto/worker.proto
	python3 -m grpc_tools.protoc -Imapreduce/proto=mapreduce/proto --python_out=. --grpc_python_out=. $^

.PHONY: all clean

clean:
	rm -f mapreduce/proto/*.py

