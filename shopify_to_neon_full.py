import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

SHOP_NAME = os.getenv("SHOP_NAME")
ACCESS_TOKEN = os.getenv("SHOPIFY_TOKEN")
NEON_URL = os.getenv("NEON_URL")

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

API_VERSION = "2023-10"

def fetch_data(endpoint, params=None):
    url = f"https://{SHOP_NAME}/admin/api/{API_VERSION}/{endpoint}.json"
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro {response.status_code} ao aceder a {endpoint}")
        return {}

def sync_table(df, table_name, id_column):
    engine = create_engine(NEON_URL)
    with engine.connect() as conn:
        existing = pd.read_sql(f"SELECT {id_column} FROM {table_name}", conn) if engine.dialect.has_table(conn, table_name) else pd.DataFrame(columns=[id_column])
        novos = df[~df[id_column].isin(existing[id_column])]
        if not novos.empty:
            try:
                novos.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"✅ {len(novos)} novos registos adicionados a {table_name}")
            except Exception as e:
                print(f"❌ Erro ao escrever em {table_name}:", e)

def process_orders():
    orders = fetch_data('orders', {'limit': 250, 'status': 'any'})
    df = pd.json_normalize(orders.get('orders', []))
    df = df[[
        'id', 'created_at', 'total_price', 'currency',
        'shipping_address.city', 'shipping_address.country',
        'shipping_address.zip', 'shipping_address.address1'
    ]].rename(columns={
        'id': 'order_id',
        'shipping_address.city': 'city',
        'shipping_address.country': 'country',
        'shipping_address.zip': 'zip',
        'shipping_address.address1': 'address'
    })
    sync_table(df, 'orders', 'order_id')

if __name__ == "__main__":
    process_orders()
