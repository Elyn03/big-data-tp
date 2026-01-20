# dashboard/app.py
import time
import requests
import pandas as pd
import streamlit as st

API_URL = "http://localhost:8000"

st.title("Dashboard KPIs")

# API connectivity test
st.sidebar.markdown("### Status API")
resp_health = requests.get(f"{API_URL}/health", timeout=5)
if resp_health.status_code == 200:
    st.sidebar.success("✅ API OK")
else:
    st.sidebar.error("❌ API indisponible")

tabs = st.tabs(["Distribution Globale", "Clients par pays", "CA par pays"])

# TAB 1
with tabs[0]:
    st.subheader("Statistiques Globales")

    col1, col2 = st.columns(2)
    row1_col1, row1_col2 = st.columns(2)
    start_time = time.perf_counter()
    resp = requests.get(f"{API_URL}/distribution_global", timeout=10)
    elapsed_api = resp.elapsed.total_seconds()

    if resp.status_code == 200 and resp.json():
        data = resp.json()
        df_distri = pd.DataFrame(data)

        with col1:
            st.metric("Total Ventes", f"{df_distri['total_ventes'][0]:,}")
        with col2:
            st.metric("Chiffre d'Affaires", f"€{df_distri['total_chiffre_affaires'][0]:,.2f}")
        with row1_col1:
            st.metric("Panier Minimum", f"€{df_distri['min_depense'][0]:.2f}")
        with row1_col2:
            st.metric("Panier Maximum", f"€{df_distri['max_depense'][0]:.2f}")

    else:
        st.warning("⚠️ Endpoint /distribution_global non disponible")
        st.info("Assure-toi d'avoir généré `distribution_global.parquet` avec le pipeline Gold")

    colAPI, colTotal = st.columns(2)
    with colAPI:
        st.metric("⏱️ Temps API", f"{elapsed_api:.3f}s")
    with colTotal:
        st.metric("⏱️ Temps total", f"{time.perf_counter() - start_time:.3f}s")


# TAB 2
with tabs[1]:
    st.subheader("Clients par pays et année")
    start_time = time.perf_counter()
    resp = requests.get(f"{API_URL}/clients_by_year_country", timeout=10)
    elapsed_api = resp.elapsed.total_seconds()

    if resp.status_code == 200:
        data = resp.json()
        df_clients = pd.DataFrame(data)
        st.dataframe(df_clients, use_container_width=True)
    else:
        st.error(f"❌ Erreur API /clients_by_year_country")

    colAPI, colTotal = st.columns(2)
    with colAPI:
        st.metric("⏱️ Temps API", f"{elapsed_api:.3f}s")
    with colTotal:
        st.metric("⏱️ Temps total", f"{time.perf_counter() - start_time:.3f}s")


# TAB 3
with tabs[2]:
    st.subheader("Chiffre d'affaires par pays et période")
    period_ca = st.selectbox(
        "Choisir la période",
        ["Année", "Mois", "Jour"],
        key="select_ca_period"
    )

    endpoint_map_ca = {
        "Année": "ca_by_year_country",
        "Mois": "ca_by_month_country",
        "Jour": "ca_by_day_country"
    }

    start_time = time.perf_counter()
    resp = requests.get(f"{API_URL}/{endpoint_map_ca[period_ca]}", timeout=10)
    elapsed_api = resp.elapsed.total_seconds()

    if resp.status_code == 200:
        data = resp.json()
        df_ca = pd.DataFrame(data)
        st.dataframe(df_ca, use_container_width=True)
    else:
        st.error(f"❌ Erreur API {endpoint_map_ca[period_ca]}")

    colAPI, colTotal = st.columns(2)
    with colAPI:
        st.metric("⏱️ Temps API", f"{elapsed_api:.3f}s")
    with colTotal:
        st.metric("⏱️ Temps total", f"{time.perf_counter() - start_time:.3f}s")
