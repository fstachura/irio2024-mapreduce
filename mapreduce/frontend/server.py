import os
import grpc
import requests
import logging
from flask import Flask, render_template, request, redirect, send_file, flash
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from google.protobuf.empty_pb2 import Empty
from google.cloud import storage

from mapreduce import worker

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
app.secret_key = os.environ.get("SECRET_KEY")

def get_status(coordinator_addr):
    with grpc.insecure_channel(coordinator_addr) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        return stub.LastJobStatus(Empty())

def get_file_upload_url(coordinator_addr, num, direct=False):
    with grpc.insecure_channel(coordinator_addr) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        return stub.UploadFiles(coordinator_pb2.UploadFilesRequest(numberOfFiles=num, withoutDirectory=direct))

def start_job(coordinator_addr, input_location, coordinator_code_location, worker_code_location):
    with grpc.insecure_channel(coordinator_addr) as channel:
        stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
        result = stub.StartJob(
                coordinator_pb2.StartJobRequest(
                    inputLocation=input_location,
                    outputLocation=input_location.rstrip("/") + "_output",
                    coordinatorCodeLocation=coordinator_code_location,
                    workerCodeLocation=worker_code_location,
                )
            )

def upload_code_file(code):
    upload_result = get_file_upload_url(app.config["COORDINATOR_ADDR"], 1, True)
    result = requests.put(upload_result.uploadUrls[0],
                          code.read(),
                          headers={'Content-Type': 'application/octet-stream'})
    if result.status_code != 200:
        return None, result
    else:
        return upload_result.inputLocation[0], None

@app.route("/")
@auth.login_required
def index():
    last_job = get_status(app.config["COORDINATOR_ADDR"])
    return render_template("index.html", last_job=last_job)

@app.route("/download_last")
@auth.login_required
def download_last():
    last_job = get_status(app.config["COORDINATOR_ADDR"])
    storage_client = storage.Client()
    bucket_name, filename = last_job.status.outputLocation.split(':')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(filename)
    if blob is None:
        flash("output file is not ready yet")
        return redirect("/")

    return send_file(blob.open('rb'), as_attachment=True, download_name="mapreduce_output")

@app.route("/start", methods=["POST"])
@auth.login_required
def start():
    files = request.files.getlist("file")
    coordinator_code = request.files.get("coordinator_code_file")
    worker_code = request.files.get("worker_code_file")
    coordinator_code_location, worker_code_location = None, None

    failed = []

    if coordinator_code is not None and len(coordinator_code.filename) != 0:
        coordinator_code_location, upload_result = upload_code_file(coordinator_code)
        if upload_result is not None:
            failed.append((coordinator_code.filename, upload_result.status_code, upload_result.text))

    if worker_code is not None and len(worker_code.filename) != 0:
        worker_code_location, upload_result = upload_code_file(worker_code)
        if upload_result is not None:
            failed.append((worker_code.filename, upload_result.status_code, upload_result.text))

    uploadResult = get_file_upload_url(app.config["COORDINATOR_ADDR"], len(files))

    for (f, url) in zip(files, uploadResult.uploadUrls):
        result = requests.put(url, f.read(), headers={'Content-Type': 'application/octet-stream'})
        if result.status_code != 200:
            failed.append((f.filename, result.status_code, result.text))

    if len(failed) == 0:
        try:
            start_job(
                app.config["COORDINATOR_ADDR"],
                uploadResult.inputLocation[0],
                coordinator_code_location,
                worker_code_location,
            )
            flash("files uploaded successfully, job started. inputlocation: " + uploadResult.inputLocation[0])
            return redirect("/")
        except Exception:
            logging.exception("failed to start job")
            flash("failed to start job, check logs")
            return redirect("/")
    else:
        flash("failed to upload some files: " + str(failed))
        return redirect("/")

application = app

