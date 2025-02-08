import datetime
import uuid
from math import ceil
from google.cloud import storage
from sqlalchemy import select

from .database import Job, JobPart

def split_on_same_elements(l, parts, eq):
    if parts == 1:
        return [l]

    result = []
    part_len = ceil(len(l)/parts)
    part_end_idxes = [(n+1)*part_len for n in range(parts-1)]

    for n in range(parts-1):
        i = part_end_idxes[n]
        while i < len(l) and eq(l[i-1], l[i]):
            i += 1
        part_end_idxes[n] = i

    start = 0
    for idx in part_end_idxes:
        result.append(l[start:idx])
        start = idx

    result.append(l[part_end_idxes[-1]:])
    return result

def generate_tmp_location(base, step):
    return base.rstrip("/") + f"_tmp/{step}/" + str(uuid.uuid4())

def get_blob(location):
    storage_client = storage.Client()
    bucket_name, filename = location.split(':')
    bucket = storage_client.bucket(bucket_name)
    return bucket.get_blob(filename)

def generate_upload_url(bucket, filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(filename)

    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(hours=1),
        method="PUT",
        content_type="application/octet-stream"
    )

    return url

def get_unfinished_job_parts(tr):
    stmt = select(JobPart).where(JobPart.finished == False)
    return tr.execute(stmt).scalars().all()

def get_unfinished_job(tr):
    stmt = select(Job).where(Job.finished == False)
    return tr.execute(stmt).scalars().first()

