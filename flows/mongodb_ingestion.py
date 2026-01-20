from io import BytesIO
from datetime import datetime

from pathlib import Path
import pandas as pd
from pymongo import MongoClient
from pandas.api.types import is_period_dtype

from config import BUCKET_GOLD, get_minio_client, URI_MONGO_DB


def load_gold_table(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, object_name)

    data = BytesIO(response.read())
    df = pd.read_parquet(data)

    response.close()
    response.release_conn()
    return df

def create_mongo_connection():
    client = MongoClient(URI_MONGO_DB)
    db = client["mongo_ingestion_db"]
    return db

def convert_df_to_records(df: pd.DataFrame) -> list:
    # convert Period to string
    for col in df.columns:
        if is_period_dtype(df[col]):
            df[col] = df[col].astype(str)
    
    df["last_updated"] = datetime.now()
    records = df.to_dict(orient="records")
    return records

def mongodb_transformation_flow():
    db = create_mongo_connection()
    data = ingestion_data()

    for d in data:
        df_kpi = load_gold_table(f"{d}.parquet")
        records = convert_df_to_records(df_kpi)
        collection = db[d]
        collection.delete_many({})
        collection.insert_many(records)

    print("finish")

def ingestion_data():
    return [
        "clients_by_year_country",
        "ca_by_year_country",
        "ca_by_month_country",
        "ca_by_day_country",
        "clients_growth_by_year",
        "ca_growth_by_year"
    ]

if __name__ == "__main__":
    mongodb_transformation_flow()
