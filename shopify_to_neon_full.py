import os
import pandas as pd
import requests
from sqlalchemy import create_engine
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

# Função com paginação para buscar todas as encomendas
def fetch_all_orders():
    all_orders = []
    url = f"https://{SHOP_NAME}/admin/api/{API_VERSION}/orders.json?limit=250&status=any"
    
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"❌ Erro {response.status_code} ao aceder a {url}")
            break

        data = response.json().get('orders', [])
        all_orders.extend(data)

        # Verificar se há próxima página
        link = response.headers.get('Link')
        if link and 'rel="next"' in link:
            next_url = [l for l in link.split(',') if 'rel="next"' in l]
            if next_url:
                url = next_url[0].split(';')[0].strip().strip('<>')
            else:
                url = None
        else:
            url = None

    return all_orders

# Sincronizar dados com Neon
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
        else:
            print(f"ℹ️ Nenhum novo registo para adicionar a {table_name}")

# Processar encomendas
def process_orders():
    orders = fetch_all_orders()
    if not orders:
        print("ℹ️ Nenhuma encomenda encontrada.")
        return

    df = pd.json_normalize(orders)
    
    # Apenas as colunas relevantes
    df = df[[
        'id', 'created_at', 'total_price', 'currency',
        'shipping_address.country'
    ]].rename(columns={
        'id': 'order_id',
        'shipping_address.country': 'country'
    })

    sync_table(df, 'orders', 'order_id')

# Executar
if __name__ == "__main__":
    process_orders()
