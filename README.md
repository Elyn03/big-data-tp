# Base NoSQL opérationnelle avec MongoDB
- Pipeline qui lit Gold (Parquet) et écrit dans MongoDB
- API Flask/FastAPI qui expose les données MongoDB Dashboard Streamlit qui interroge l'API
- Calculer le temps de refresh entre les 2
- (Bonus : Mise en place de metadata pour faire un dashboard)


## Etapes à suivre

### 1. Prérequis
- Environnement virtuel (.venv)
- Python 3.10+
- Docker (optionnel pour Metabase ou d’autres conteneurs)
- MongoDB Atlas ou serveur MongoDB local
- Streamlit

### 2. Installer les dépendances
```bash
pip install -r requirements.txt
```

### 3. Configuration
- Ajouter un .env
- Renseigner le URI MongoDB

### 4. Lancer le projet
```bash
uvicorn api.main:app --reload --port 8000
streamlit run dashboard/app.py
```

### Bonus

```bash
docker rm -f metabase
docker pull metabase/metabase:latest 
docker run -d -p 3000:3000 --name metabase metabase/metabase
```