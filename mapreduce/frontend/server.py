import os
import grpc
import requests
import logging
from flask import Flask, render_template, request, redirect, send_file
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from google.protobuf.empty_pb2 import Empty
from google.cloud import storage

from ..proto import coordinator_pb2
from ..proto import coordinator_pb2_grpc

auth_user = os.environ["AUTH_USER"]
auth_pass = generate_password_hash(os.environ["AUTH_PASS"])
auth = HTTPBasicAuth()

@auth.verify_password
def verify_password(username, password):
    if username == auth_user and check_password_hash(auth_pass, password):
        return username

app = Flask(__name__)
app.config["COORDINATOR_ADDR"] = os.environ.get("COORDINATOR_ADDR", "coordinator.default.svc.cluster.local")

def get_status(coordinator_addr):
    with grpc.insecure_channel(coordinator_addr) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        return stub.LastJobStatus(Empty())

def get_file_upload_url(coordinator_addr, num):
    with grpc.insecure_channel(coordinator_addr) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        return stub.UploadFiles(coordinator_pb2.UploadFilesRequest(numberOfFiles=num))

def start_job(coordinator_addr, input_location):
    with grpc.insecure_channel(coordinator_addr) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        result = stub.StartJob(
                coordinator_pb2.StartJobRequest(
                    inputLocation=input_location,
                    outputLocation=input_location.rstrip("/") + "_output"
                )
            )

def generate_index(msg=""):
    last_job = get_status(app.config["COORDINATOR_ADDR"])
    return render_template("index.html", last_job=last_job, msg=msg)

@app.route("/")
@auth.login_required
def index():
    return generate_index()

@app.route("/download_last")
@auth.login_required
def download_last():
    last_job = get_status(app.config["COORDINATOR_ADDR"])
    storage_client = storage.Client()
    bucket_name, filename = last_job.status.outputLocation.split(':')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(filename)
    if blob is None:
        return generate_index("output file is not ready yet")

    return send_file(blob.open('rb'), as_attachment=True, download_name="mapreduce_output")

@app.route("/start", methods=["POST"])
@auth.login_required
def start():
    files = request.files.getlist("file")

    uploadResult = get_file_upload_url(app.config["COORDINATOR_ADDR"], len(files))
    failed = []

    for (f, url) in zip(files, uploadResult.uploadUrls):
        result = requests.put(url, f.read(), headers={'Content-Type': 'application/octet-stream'})
        if result.status_code != 200:
            failed.append((f.filename, result.status_code, result.text))

    if len(failed) == 0:
        try:
            start_job(app.config["COORDINATOR_ADDR"], uploadResult.inputLocation)
            return generate_index("files uploaded successfully, job started. inputlocation: " + uploadResult.inputLocation)
        except Exception:
            logging.exception("failed to start job")
            return generate_index("failed to start job, check logs")
    else:
        return generate_index("failed to upload some files: " + str(failed))

application = app

