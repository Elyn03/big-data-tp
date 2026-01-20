from pathlib import Path
from io import BytesIO
import pandas as pd
from minio import Minio
from prefect import flow, task
from config import BUCKET_GOLD, BUCKET_SILVER, get_minio_client

@task(name="load_silver_table")
def load_silver_table(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)
    df = pd.read_csv(response)

    response.close()
    response.release_conn()
    return df

@task(name="kpi_clients_by_year_and_country")
def kpi_clients_by_year_and_country(df_clients: pd.DataFrame) -> pd.DataFrame:
    df_clients["annee_inscription"] = df_clients["date_inscription"].dt.year

    clients_by_year_country = (
        df_clients
        .groupby(["annee_inscription", "pays"])["id_client"]
        .nunique()
        .reset_index(name="nb_clients")
        .sort_values(["annee_inscription", "pays"])
    )
    return clients_by_year_country

@task(name="kpi_clients_by_period")
def kpi_clients_by_period(df_clients: pd.DataFrame, period: str, period_code: str) -> pd.DataFrame:
    clients_by_period = (
        df_clients.groupby(df_clients["date_inscription"].dt.to_period(period_code))
        .size()
        .reset_index(name="nb_clients")
    )
    return clients_by_period

@task(name="kpi_ca_by_period")
def kpi_ca_by_period(df_joined: pd.DataFrame, period: str, period_code: str) -> pd.DataFrame:
    df_joined[period] = df_joined["date_achat"].dt.to_period(period_code)
    ca_by_month = (
        df_joined
        .groupby([period])["montant"]
        .sum()
        .reset_index(name="chiffre_affaires")
        .sort_values([period], ascending=[True])
    )
    return ca_by_month

@task(name="kpi_ca_by_period_and_country")
def kpi_ca_by_period_and_country(df_joined: pd.DataFrame, period: str, period_code: str) -> pd.DataFrame:
    df_joined[period] = df_joined["date_achat"].dt.to_period(period_code)
    ca_by_period_country = (
        df_joined
        .groupby([period, "pays"])["montant"]
        .sum()
        .reset_index(name="chiffre_affaires")
        .sort_values([period, "chiffre_affaires"], ascending=[True, False])
    )
    return ca_by_period_country

@task(name="kpi_growth_rate")
def kpi_growth_rate(df: pd.DataFrame, value_col: str, period_col: str) -> pd.DataFrame:
    df = df.sort_values(period_col)
    df["taux_croissance"] = df[value_col].pct_change()
    return df


@task(name="gold_transformation")
def gold_transformation():
    df_clients = load_silver_table("clients.parquet")
    df_achats = load_silver_table("achats.parquet")
    df_clients["date_inscription"] = pd.to_datetime(df_clients["date_inscription"])
    df_achats["date_achat"] = pd.to_datetime(df_achats["date_achat"])
    df_joined = df_achats.merge(
        df_clients[["id_client", "pays"]],
        on="id_client",
        how="left"
    )
    # KPI : nombre de clients
    clients_by_year_country = kpi_clients_by_year_and_country(df_clients) # par année et par pays
    clients_by_year = kpi_clients_by_period(df_clients, "annee", "Y") # par année
    clients_by_month = kpi_clients_by_period(df_clients, "mois", "M") # par mois
    clients_by_week = kpi_clients_by_period(df_clients, "semaine", "W") # par semaine
    clients_by_day = kpi_clients_by_period(df_clients, "jour", "D") # par jour

    # KPI : chiffre d'affaires
    ca_by_year = kpi_ca_by_period(df_joined, "annee", "Y") # par année
    ca_by_month = kpi_ca_by_period(df_joined, "mois", "M") # par mois
    ca_by_week = kpi_ca_by_period(df_joined, "semaine", "W") # par semaine
    ca_by_day = kpi_ca_by_period(df_joined, "jour", "D") # par jour

    # KPI : chiffre d'affaires par pays
    ca_by_year_country = kpi_ca_by_period_and_country(df_joined, "annee", "Y") # par année
    ca_by_month_country = kpi_ca_by_period_and_country(df_joined, "mois", "M") # par mois
    ca_by_week_country = kpi_ca_by_period_and_country(df_joined, "semaine", "W") # par semaine
    ca_by_day_country = kpi_ca_by_period_and_country(df_joined, "jour", "D") # par jour

    # KPI : taux de croissance
    clients_growth_by_year = kpi_growth_rate(clients_by_year_country, "nb_clients", "annee_inscription")

    ca_growth_by_year = kpi_growth_rate(ca_by_year, "chiffre_affaires", "annee")
    ca_growth_by_month = kpi_growth_rate(ca_by_month, "chiffre_affaires", "mois")
    ca_growth_by_week = kpi_growth_rate(ca_by_week, "chiffre_affaires", "semaine")
    ca_growth_by_day = kpi_growth_rate(ca_by_day, "chiffre_affaires", "jour")

    # Sauvegarde
    save_gold_table(clients_by_year_country, "clients_by_year_country.parquet")
    save_gold_table(ca_by_year_country, "ca_by_year_country.parquet")
    save_gold_table(ca_by_month_country, "ca_by_month_country.parquet")
    save_gold_table(ca_by_day_country, "ca_by_day_country.parquet")
    save_gold_table(clients_growth_by_year, "clients_growth_by_year.parquet")
    save_gold_table(ca_growth_by_year, "ca_growth_by_year.parquet")

@task(name="save_gold_table")
def save_gold_table(df: pd.DataFrame, object_name: str):
    client = get_minio_client()

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client.put_object(
        BUCKET_GOLD,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

if __name__ == "__main__":
    gold_transformation()
