from io import BytesIO
from pathlib import Path
from pandas import pandas as pd
from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client

def data_clean(df: pd.DataFrame, object_name: str) -> pd.DataFrame:
    df = df.dropna(how='all')
    if (object_name == "clients.csv"):
        df = df.dropna(subset=['id_client'], how='any')
        df = df.dropna(subset=['email'], how='any')
        df = df.drop_duplicates(subset=['id_client'], keep='first')
        df = df.drop_duplicates(subset=['email'], keep='first')
    if (object_name == "achats.csv"):
        df = df.dropna(subset=['id_achat'], how='any')
    return df

def standardize_dates(df: pd.DataFrame, object_name: str) -> pd.DataFrame:
    if (object_name == "clients.csv"):
        df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce").dt.strftime("%Y-%m-%d")
    if (object_name == "achats.csv"):
        df["date_achat"] = pd.to_datetime(df["date_achat"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df

def normalize_data_types(df: pd.DataFrame, object_name: str) -> pd.DataFrame:
    if (object_name == "clients.csv"):
        df["id_client"] = df["id_client"].astype(int)
        df["nom"] = df["nom"].astype(str)
        df["email"] = df["email"].astype(str)
        df["date_inscription"] = df["date_inscription"].astype(str)
        df["pays"] = df["pays"].astype(str)
    if (object_name == "achats.csv"):
        df["id_achat"] = df["id_achat"].astype(int)
        df["id_client"] = df["id_client"].astype(int)
        df["date_achat"] = df["date_achat"].astype(str)
        df["montant"] = df["montant"].astype(float)
        df["produit"] = df["produit"].astype(str)
    return df

def silver_transformation(object_name: str) -> str:
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)

    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(BytesIO(data))

    # Nettoyer les valeurs nulles et aberrantes
    df = data_clean(df, object_name)

    # Standardiser les formats de dates
    df = standardize_dates(df, object_name)

    # Normaliser les types de données
    df = normalize_data_types(df, object_name)

    object_name_parquet = object_name.replace(".csv", ".parquet")
    client.put_object(
        BUCKET_SILVER,
        object_name_parquet,
        BytesIO(data),
        length=len(data)
    )

    return df
    
def silver_transformation_flow(data_dir: str = "./data/sources") -> dict:
    silver_clients = silver_transformation("clients.csv")
    silver_achats = silver_transformation("achats.csv")

    return {
        "clients": silver_clients,
        "achats": silver_achats
    }

if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"Silver transformation complete: {result}")
