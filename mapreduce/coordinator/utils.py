import uuid
from google.cloud import storage
from sqlalchemy import select

from .database import Job, JobPart

def split_on_same_elements(l, parts, eq):
    result = []
    part_len = (len(l)+1)//parts
    part_idxes = [(n+1)*part_len for n in range(parts-1)]

    for n in range(parts-1):
        i = part_idxes[n]
        while i < len(l) and eq(l[i-1], l[i]):
            i += 1
        part_idxes[n] = i

    start = 0
    for idx in part_idxes:
        result.append(l[start:idx])
        start = idx

    result.append(l[part_idxes[-1]:])
    return result

def generate_tmp_location(base, step):
    return base.rstrip("/") + f"_tmp/{step}/" + str(uuid.uuid4())

def get_blob(location):
    storage_client = storage.Client()
    bucket_name, filename = location.split(':')
    bucket = storage_client.bucket(bucket_name)
    return bucket.get_blob(filename)

def get_unfinished_job_parts(tr):
    stmt = select(JobPart).where(JobPart.finished == False)
    return tr.execute(stmt).scalars().all()

def get_unfinished_job(tr):
    stmt = select(Job).where(Job.finished == False)
    return tr.execute(stmt).scalars().first()

