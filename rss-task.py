from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import sqlite3
import os
import xml.etree.ElementTree as ET
import logging as logger

WORKING_DIR = "/mnt/c/Users/vishn/OneDrive/Desktop/study/Tryouts/2023/airflow/output"

if os.path.isdir(WORKING_DIR):
    os.chdir(WORKING_DIR)
else:
    os.makedirs(WORKING_DIR)
    os.chdir(WORKING_DIR)

DB_DIR = "db"
RAW_DIR = "raw"
CURATED_DIR = "curated"

DB_NAME = "rss_feed.sqlite"
DB_TABLE = "rss_feed"
DB_PATH = os.path.join(WORKING_DIR, DB_DIR, DB_NAME)

if not os.path.exists("db"):
    os.makedirs("db")


default_args = {
    "owner": "Vishnu MK",
    "start_date": datetime(2023, 7, 18, 23, 0, 0),
}


def download_rss_feed() -> tuple[str, datetime]:
    logger.info("Downloading RSS feed")

    timestamp = datetime.now()
    timestampStr = timestamp.strftime("%Y%m%d%H%M%S")

    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)

    filename = f"raw_rss_feed_{timestampStr}.xml"
    filepath = os.path.join(WORKING_DIR, RAW_DIR, filename)
    url = "https://timesofindia.indiatimes.com/rssfeedstopstories.cms"

    response = requests.get(url)
    with open(filepath, "wb+") as file:
        file.write(response.content)

    logger.info("Downloaded RSS feed")

    return filepath, timestamp


def parse_rss_feed(ti) -> str:
    logger.info("Parsing RSS feed")

    filepath, timeStamp = ti.xcom_pull(
        task_ids="download_rss_feed_task", key="return_value"
    )

    # filepath, timeStamp = download_rss_feed()

    # Parse the XML file and extract the desired information
    # Save the extracted information in a CSV file
    # Parse the XML file
    with open(filepath, "r", encoding="utf8") as file:
        tree = ET.parse(file)

    root = tree.getroot()

    # Extract the desired information (e.g., title and description)
    data = []
    for item in root.findall(".//item"):
        item_data = {}
        for child in item:
            item_data[child.tag] = child.text
        data.append(item_data)

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data)

    # Drop the columns that are not required
    required_columns = ["title", "description", "link", "pubDate", "guid"]

    df = df[required_columns]

    # Save the DataFrame to a CSV file
    timestamp = timeStamp.strftime("%Y%m%d%H%M%S")
    csv_filename = f"curated_{timestamp}.csv"
    csv_filepath = os.path.join(WORKING_DIR, CURATED_DIR, csv_filename)

    logger.info("Parsed RSS feed. Saving to CSV")

    if not os.path.exists(CURATED_DIR):
        os.makedirs(CURATED_DIR)

    df.to_csv(csv_filepath, index=False)

    return csv_filepath


def load_to_database(ti):
    filename = ti.xcom_pull(task_ids="parse_rss_feed_task", key="return_value")

    logger.info("Loading to database")

    # filename = parse_rss_feed()

    with open(filename, "r", encoding="utf8") as file:
        df = pd.read_csv(file)

    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(DB_TABLE, conn, if_exists="append", index=False)

    logger.info("Loaded to database")


with DAG(
    "rss_feed_dag", default_args=default_args, schedule_interval="0 23 * * *"
) as dag:
    download_rss_feed_task = PythonOperator(
        task_id="download_rss_feed_task",
        python_callable=download_rss_feed,
    )

    parse_rss_feed_task = PythonOperator(
        task_id="parse_rss_feed_task",
        python_callable=parse_rss_feed,
    )

    load_to_database_task = PythonOperator(
        task_id="load_to_database_task",
        python_callable=load_to_database,
    )

    download_rss_feed_task >> parse_rss_feed_task >> load_to_database_task
