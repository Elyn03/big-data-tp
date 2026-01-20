# dashboard/app.py
import time
import requests
import pandas as pd
import streamlit as st

API_URL = "http://localhost:8000"

st.title("Dashboard KPIs - Gold Layer")

# Onglets
tabs = st.tabs(["Growth général", "Chiffre d'affaires", "Clients par pays"])

# -----------------------------
# Onglet 1 : Growth général
# -----------------------------
with tabs[0]:
    st.subheader("Growth général")
    growth_type = st.selectbox("Type de growth", ["Clients", "CA"])
    
    endpoint_map_growth = {
        "Clients": "clients_growth_by_year",
        "CA": "ca_growth_by_year"
    }
    
    start_time = time.perf_counter()
    resp = requests.get(f"{API_URL}/{endpoint_map_growth[growth_type]}")
    elapsed_api = resp.elapsed.total_seconds()
    if resp.status_code == 200:
        data = resp.json()
        df_growth = pd.DataFrame(data)
        st.dataframe(df_growth)
    else:
        st.error(f"Erreur API {endpoint_map_growth[growth_type]}")
    st.metric("Temps API (s)", f"{elapsed_api:.3f}")
    st.metric("Temps total refresh (s)", f"{time.perf_counter() - start_time:.3f}")


# -----------------------------
# Onglet 2 : Chiffre d'affaires
# -----------------------------
with tabs[1]:
    st.subheader("Chiffre d'affaires par période et pays")
    period_choice = st.selectbox("Choisir la période", ["Année", "Mois", "Jour"])
    
    endpoint_map = {
        "Année": "ca_by_year_country",
        "Mois": "ca_by_month_country",
        "Jour": "ca_by_day_country"
    }
    
    start_time = time.perf_counter()
    resp = requests.get(f"{API_URL}/{endpoint_map[period_choice]}")
    elapsed_api = resp.elapsed.total_seconds()
    if resp.status_code == 200:
        data = resp.json()
        df_ca = pd.DataFrame(data)
        st.dataframe(df_ca)
    else:
        st.error(f"Erreur API {endpoint_map[period_choice]}")
    st.metric("Temps API (s)", f"{elapsed_api:.3f}")
    st.metric("Temps total refresh (s)", f"{time.perf_counter() - start_time:.3f}")

# -----------------------------
# Onglet 3 : Clients par pays
# -----------------------------
with tabs[2]:
    st.subheader("Clients par pays et par année")
    start_time = time.perf_counter()
    resp = requests.get(f"{API_URL}/clients_by_year_country")
    elapsed_api = resp.elapsed.total_seconds()
    if resp.status_code == 200:
        data = resp.json()
        df_clients = pd.DataFrame(data)
        st.dataframe(df_clients)
    else:
        st.error("Erreur API clients_by_year_country")
    st.metric("Temps API (s)", f"{elapsed_api:.3f}")
    st.metric("Temps total refresh (s)", f"{time.perf_counter() - start_time:.3f}")
