import os
import logging
from typing import List
import uuid
import enum
import sqlalchemy
from sqlalchemy import UUID, Boolean, ForeignKey, String, Enum, MetaData, null
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

# Each node has UUID, UUID is generated on start
# In a loop query nodes returned by k8s DNS for map node uuid:(part, state)
# If the difference between UUIDs from JobPart of current step and returned UUIDs is not empty,
#   assign job parts of missing nodes to new nodes

# Tracks part execution and which node executes what
class JobPart(Base):
    __tablename__ = "job_part"

    id: Mapped[int] = mapped_column(primary_key=True)
    input_location: Mapped[str] = mapped_column()
    output_location: Mapped[str] = mapped_column()
    step: Mapped[str] = mapped_column()
    finished: Mapped[bool] = mapped_column(Boolean(), default=False, nullable=False)
    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"))
    executor_node_uuid: Mapped[UUID] = mapped_column(UUID(as_uuid=True), nullable=True)

class JobStatus(enum.Enum):
    in_progress = "IN_PROGRESS"
    finished = "FINISHED"
    failed = "FAILED"

# Tracks started jobs
class Job(Base):
    __tablename__ = "job"

    id: Mapped[int] = mapped_column(primary_key=True)
    input_location: Mapped[str] = mapped_column(String())
    job_uuid: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), default=uuid.uuid4)
    job_status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.in_progress)
    # NOTE: hacky way to ensure that only one job can be in progress at the same time.
    # all null values are "unique", this should be set to null when the job is actually finished
    finished: Mapped[bool] = mapped_column(Boolean(), default=False, nullable=True, unique=True)
    current_step: Mapped[str] = mapped_column()
    parts: Mapped[List["JobPart"]] = relationship()

def connect_to_db() -> sqlalchemy.engine.base.Engine:
    engine = sqlalchemy.engine.url.URL.create(
            drivername='postgresql+psycopg2',
            username=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", 5432)),
            database=os.environ["DB_NAME"],
        )

    db = sqlalchemy.create_engine(engine)
    # no migrations for now
    Base.metadata.create_all(db)
    with db.begin() as transaction:
        result = transaction.execute(sqlalchemy.text("SELECT 1"))
        assert result.all()[0][0] == 1
        logger.info("connected to the database")
    return db

