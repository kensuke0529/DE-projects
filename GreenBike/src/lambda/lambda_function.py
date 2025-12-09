"""

Station Status (real-time availability)
https://gbfs.bcycle.com/bcycle_greenbikeslc/station_status.json

Station Information (name, address, coordinates)
https://gbfs.bcycle.com/bcycle_greenbikeslc/station_information.json

"""

"""
Station Status (real-time availability)
https://gbfs.bcycle.com/bcycle_greenbikeslc/station_status.json

Station Information (name, address, coordinates)
https://gbfs.bcycle.com/bcycle_greenbikeslc/station_information.json
"""

import json
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime


def lambda_handler(event, context):
    station_status_url = (
        "https://gbfs.bcycle.com/bcycle_greenbikeslc/station_status.json"
    )
    station_info_url = (
        "https://gbfs.bcycle.com/bcycle_greenbikeslc/station_information.json"
    )

    station_status = requests.get(station_status_url).json()["data"]["stations"]
    station_info = requests.get(station_info_url).json()["data"]["stations"]

    station_status_df = pd.DataFrame(station_status)
    station_info_df = pd.DataFrame(station_info)

    station_status_df["electric"] = station_status_df[
        "num_bikes_available_types"
    ].apply(lambda x: x.get("electric", 0))
    station_status_df["classic"] = station_status_df["num_bikes_available_types"].apply(
        lambda x: x.get("classic", 0)
    )
    station_status_df["timestamp"] = pd.to_datetime(
        station_status_df["last_reported"], unit="s"
    )

    station_status_df = station_status_df.drop(
        columns=[
            "num_bikes_available_types",
            "last_reported",
        ]
    )

    df = pd.merge(station_status_df, station_info_df, on="station_id")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    now = datetime.utcnow()
    key = f"raw-data/{now.strftime('%Y/%m/%d/%H%M')}.csv"

    s3 = boto3.client("s3")
    s3.put_object(Bucket="greenbike-data", Key=key, Body=csv_buffer.getvalue())

    return {
        "statusCode": 200,
        "body": json.dumps("Success"),
    }


if __name__ == "__main__":
    lambda_handler(None, None)
