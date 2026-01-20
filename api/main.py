# api/main.py
from datetime import datetime
from typing import List
import sys
from fastapi import FastAPI
from pymongo import MongoClient
from bson import ObjectId

sys.path.append("./flows")
from config import URI_MONGO_DB

app = FastAPI(title="Gold KPIs API")

client = MongoClient(URI_MONGO_DB)
db = client["mongo_ingestion_db"]
coll_clients_by_year_country = db["clients_by_year_country"]
coll_ca_by_year_country = db["ca_by_year_country"]
coll_ca_by_month_country = db["ca_by_month_country"]
coll_ca_by_day_country = db["ca_by_day_country"]
coll_clients_growth_by_year = db["clients_growth_by_year"]
coll_ca_growth_by_year = db["ca_growth_by_year"]
coll_distribution_global = db["distribution_global"]
coll_meta = db["metadata"]

def serialize_doc(doc):
    doc["_id"] = str(doc["_id"])
    return doc


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/clients_by_year_country")
def get_clients_by_year_country():
    docs = list(coll_clients_by_year_country.find({}))
    return [serialize_doc(d) for d in docs]

@app.get("/ca_by_year_country")
def get_ca_by_year_country():
    docs = list(coll_ca_by_year_country.find({}))
    return [serialize_doc(d) for d in docs]

@app.get("/ca_by_month_country")
def get_ca_by_month_country():
    docs = list(coll_ca_by_month_country.find({}))
    return [serialize_doc(d) for d in docs]

@app.get("/ca_by_day_country")
def get_ca_by_day_country():
    docs = list(coll_ca_by_day_country.find({}))
    return [serialize_doc(d) for d in docs]

@app.get("/clients_growth_by_year")
def get_clients_growth_by_year():
    docs = list(coll_clients_growth_by_year.find({}))
    return [serialize_doc(d) for d in docs]

@app.get("/ca_growth_by_year")
def get_ca_growth_by_year():
    docs = list(coll_ca_growth_by_year.find({}))
    return [serialize_doc(d) for d in docs]

@app.get("/distribution_global")
def get_distribution_global():
    docs = list(coll_distribution_global.find({}))
    return [serialize_doc(d) for d in docs]

@app.get("/ca_by_year_country/{pays}")
def get_ca_by_country(pays: str):
    docs = list(coll_ca_by_year_country.find({"pays": pays}))
    return [serialize_doc(d) for d in docs]

@app.get("/metadata/{table_name}")
def get_metadata(table_name: str):
    meta = coll_meta.find_one({"table": table_name})
    if not meta:
        return {}
    return serialize_doc(meta)
