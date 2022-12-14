import argparse
import logging
import socket
from datetime import timedelta

import dateutil.parser
import pandas as pd
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from pythonjsonlogger import jsonlogger
from sqlalchemy import Column, Date, Float, Integer, MetaData, String, Table, create_engine

from settings import settings

logger = logging.getLogger()
logHandler = logging.StreamHandler()
logFmt = jsonlogger.JsonFormatter(timestamp=True)
logHandler.setFormatter(logFmt)
logger.addHandler(logHandler)

credential = DefaultAzureCredential()
client = LogsQueryClient(credential)

dest_db_apistats = create_engine(settings.pg_connection_string.replace("/postgres?", "/apistats?"))
meta = MetaData(dest_db_apistats)
stats_table = Table(
    "api_stats_v2",
    meta,
    Column("id", String, primary_key=True),
    Column("date", Date),
    Column("method", String),
    Column("host", String),
    Column("path", String),
    Column("status", Integer),
    Column("response_time", Float),
    Column("user_agent", String),
)


def is_leader():
    if settings.leader_election_enabled:
        r = redis.Redis.from_url(settings.redis_connection_string)
        lock_key = "loganalytics-to-tableau-lock"
        hostname = socket.gethostname()
        is_leader = False

        with r.pipeline() as pipe:
            try:
                pipe.watch(lock_key)
                leader_host = pipe.get(lock_key)
                if leader_host in (hostname.encode(), None):
                    pipe.multi()
                    pipe.setex(lock_key, 10, hostname)
                    pipe.execute()
                    is_leader = True
            except redis.WatchError:
                pass
    else:
        is_leader = True
    return is_leader


def sync() -> None:
    if not is_leader():
        return

    logging.warning("Creating tables")
    meta.create_all()
    logging.warning("Insterting results")

    query = """
    AzureDiagnostics
    | where Category == "FrontDoorAccessLog"
    | where requestUri_s startswith "https://api.gb.bink.com:443/ubiquity" or requestUri_s startswith "https://api.gb.bink.com:443/v2"
    | extend url = tostring(parse_url(requestUri_s)["Path"])
    | extend hash_cleanup = replace_regex(url, @"hash-.+", @"{id}")
    | extend sanitised = replace_regex(hash_cleanup, @"/\d+", @"{id}")
    | extend endpoint = strcat(sanitised)
    | extend timetaken = todouble(timeTaken_s)
    | where userAgent_s != "Checkly/1.0 (https://www.checklyhq.com)"
    | project
        _ItemId,
        TimeGenerated,
        timetaken,
        httpStatusCode_d,
        endpoint,
        originName_s,
        httpMethod_s,
        userAgent_s
    """

    response = client.query_workspace(workspace_id=settings.workspace_id, query=query, timespan=timedelta(days=1))

    if response.status == LogsQueryStatus.PARTIAL:
        error = response.partial_error
        data = response.partial_data
        logging.warning(error.message)
    elif response.status == LogsQueryStatus.SUCCESS:
        data = response.tables
    for table in data:
        df = pd.DataFrame(data=table.rows, columns=table.columns)

    df.reset_index()
    with dest_db_apistats.connect() as conn:
        counter = 0
        for _, row in df.iterrows():
            record = {
                "id": row["_ItemId"],
                "date": dateutil.parser.parse(str(row["TimeGenerated"])),
                "method": row["httpMethod_s"],
                "host": row["originName_s"],
                "path": row["endpoint"],
                "status": row["httpStatusCode_d"],
                "response_time": row["timetaken"],
                "user_agent": row["userAgent_s"],
            }

            insert = stats_table.insert().values(**record)
            try:
                conn.execute(insert)
            except Exception:
                pass
            counter += 1

            if counter % 100 == 0:
                logging.warning(f"Inserted {counter} values")


def main() -> None:
    logging.warning("Started Log Analytics to Tableau Syncer")
    parser = argparse.ArgumentParser()
    parser.add_argument("--now", action="store_true", help="Run Log Analytics dump now")
    args = parser.parse_args()

    if args.now:
        logging.warning("Running in run-once mode")
        sync()
    else:
        logging.warning("Running in scheduler mode")
        scheduler = BlockingScheduler()
        scheduler.add_job(sync, trigger=CronTrigger.from_crontab("0 1 * * *"))
        scheduler.start()


if __name__ == "__main__":
    main()
