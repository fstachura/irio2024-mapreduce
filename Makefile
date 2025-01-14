mapreduce/proto/mapreduce_pb2.py: mapreduce/proto/mapreduce.proto
	python3 -m grpc_tools.protoc -Imapreduce/proto=mapreduce/proto --python_out=. --grpc_python_out=. mapreduce/proto/mapreduce.proto

clean:
	rm mapreduce/proto/*.py

