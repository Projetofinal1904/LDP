import os
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

SHOP_NAME = os.getenv("SHOP_NAME")
ACCESS_TOKEN = os.getenv("SHOPIFY_TOKEN")
NEON_URL = os.getenv("NEON_URL")

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

API_VERSION = "2023-10"

# 🔄 Obter todas as encomendas com paginação
def fetch_all_orders():
    all_orders = []
    base_url = f"https://{SHOP_NAME}/admin/api/{API_VERSION}/orders.json"
    params = {
        "limit": 250,
        "status": "any",
        "order": "created_at asc"
    }
    url = base_url

    while url:
        response = requests.get(url, headers=HEADERS, params=params if '?' not in url else None)
        if response.status_code != 200:
            print(f"Erro {response.status_code} ao aceder: {url}")
            break

        data = response.json().get("orders", [])
        all_orders.extend(data)

        link_header = response.headers.get("Link", "")
        if 'rel="next"' in link_header:
            next_link = [x for x in link_header.split(",") if 'rel="next"' in x]
            if next_link:
                url = next_link[0].split(";")[0].strip().strip("<>")
                params = None
            else:
                url = None
        else:
            url = None

    return all_orders

# 🔄 Sincronizar dados na tabela (só novos registos)
def sync_table(df, table_name, id_column):
    engine = create_engine(NEON_URL)
    with engine.connect() as conn:
        if engine.dialect.has_table(conn, table_name):
            existing = pd.read_sql(f"SELECT {id_column} FROM {table_name}", conn)
        else:
            existing = pd.DataFrame(columns=[id_column])

        novos = df[~df[id_column].isin(existing[id_column])]

        # Elimina colunas inexistentes para evitar erro
        df = df[[col for col in df.columns if col in existing.columns or existing.empty]]

        if not novos.empty:
            try:
                novos.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"{len(novos)} novos registos adicionados a {table_name}")
            except Exception as e:
                print(f"Erro ao escrever em {table_name}:", e)
        else:
            print(f"Nenhum novo registo para adicionar a {table_name}")

# 🔍 Extrair produtos vendidos (line_items)
def extract_line_items(orders):
    items = []
    for order in orders:
        for item in order.get("line_items", []):
            items.append({
                "order_id": order["id"],
                "product_id": item.get("product_id"),
                "variant_id": item.get("variant_id"),
                "title": item.get("title"),
                "quantity": item.get("quantity"),
                "price": item.get("price"),
                "sku": item.get("sku"),
                # "vendor": item.get("vendor"),  # só se a coluna existir
                # "fulfillment_status": item.get("fulfillment_status")
            })
    return pd.DataFrame(items)

# 👤 Extrair clientes
def extract_customers(orders):
    customers = []
    for order in orders:
        customer = order.get("customer")
        if customer:
            customers.append({
                "customer_id": customer.get("id"),
                "first_name": customer.get("first_name"),
                "last_name": customer.get("last_name"),
                "email": customer.get("email"),
                "phone": customer.get("phone"),
                "created_at": customer.get("created_at")
            })
    return pd.DataFrame(customers).drop_duplicates(subset=["customer_id"])

# 🚀 Processo principal
def process_orders():
    orders = fetch_all_orders()
    if not orders:
        print("Nenhuma encomenda encontrada.")
        return

    # 🧾 Encomendas
    df_orders = pd.json_normalize(orders)
    df_orders = df_orders[['id', 'created_at', 'total_price', 'currency', 'shipping_address.country']].rename(columns={
        'id': 'order_id',
        'shipping_address.country': 'country'
    })
    sync_table(df_orders, 'orders', 'order_id')

    # 📦 Produtos por encomenda
    df_items = extract_line_items(orders)
    sync_table(df_items, 'line_items', 'variant_id')  # ou outra chave se necessário

    # 👤 Clientes
    df_customers = extract_customers(orders)
    sync_table(df_customers, 'customers', 'customer_id')

# ▶️ Executar script
if __name__ == "__main__":
    process_orders()



