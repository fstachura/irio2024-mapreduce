import os
import logging
import sqlalchemy

logger = logging.getLogger(__name__)

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
    with db.begin() as transaction:
        result = transaction.execute(sqlalchemy.text("SELECT 1"))
        assert result.all()[0][0] == 1
        logger.info("connected to the database")
    return db

