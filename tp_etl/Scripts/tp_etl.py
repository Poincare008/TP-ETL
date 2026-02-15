import os
import argparse
import pandas as pd
import sqlite3


Data_dir = 'sqlite_exports'
# EXTRACT - Lire les fichiers CSV
def read_csv_file(file_name):
    """
    Lire un ficher CSV avec pamdas
    
    :param file_name: Description
    """
    # Chemin pour les fichiers
    filepath = os.path.join(Data_dir,f'{file_name}.csv')

    df = pd.read_csv(filepath,low_memory=False)

    # Supprimer la colonne index si elle existe
    if "index" in df.columns:
        df = df.drop(columns=["index"])

    # Affichage des stastistiques
    print(f"\n===== {file_name} =====")
    print(f"Dimension: {df.shape}")
    print(f"Colonnes: {list(df.columns)}")

    return df



def extract():
    """
    Charge tous les CSV
    Docstring for extract
    """

    print("=" * 55)
    print("ÉTAPE 1: EXTRACT - Lecture des fichiers")
    print("=" * 55)

    # Lire tous les fichiers
    df_customers = read_csv_file('customers')
    df_orders = read_csv_file('orders')
    df_geoloc = read_csv_file('geoloc')
    df_products = read_csv_file('products')
    df_sellers = read_csv_file('sellers')
 
    df_translations = read_csv_file('translation')
    df_order_items = read_csv_file('order_items')
    df_order_pymts = read_csv_file('order_pymts')
    df_order_reviews = read_csv_file('order_reviews')

    # Retourner dans un dictionnaire

    return{
        'customers': df_customers,
        'orders': df_orders,
        'geoloc': df_geoloc,
        'products': df_products,
        'sellers': df_sellers,
        'translation': df_translations,
        'order_items': df_order_items,
        'order_pymts': df_order_pymts,
        'order_reviews': df_order_reviews

    }


# Transform - Nettoyer et Calculer

def change_date_format(df):
    """
    Docstring pour le changement du format de date
    
    :param df: Description
    """
    
    for col in df.columns:
        # Si la colonne contient date ou time dans son nom
        if 'date' in col.lower() or 'time' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
            print(f" {col} converti en date")

    return df


def duplicate_remove(df, subset_col = None):
    """
    Docstring pour suprimmer les doublons
    
    :param df: Description
    :param subset_col: Description
    """

    avant = len(df)
    if subset_col:
        df = df.drop_duplicates(subset = [subset_col])
    else:
        df = df.drop_duplicates()
    
    apres = len(df)
    removed = avant - apres

    if removed > 0:
        print(f" {removed} les doublons sont suprimes")

    return df



def transform_data(dataframes):
    """
    Docstring pour  transformer les donnees
    
    :param dataframes: Description
    """

    print('\n' + '=' * 55)
    print("ÉTAPE 2: Transform - Nettoyage et Calculs")
    print("=" * 55)

    result = {}

    # 1. Dates conversions

    print('\n1 Conversion des dates... ')
    for name, df in dataframes.items():
        dataframes[name] = change_date_format(df)


    # 2. Nettoyage de  dimensions

    print('\n2 Nettoyage de  dimensions... ')

    # Customers
    customers = duplicate_remove(dataframes['customers'],'customer_id')
    result['dim_customers'] = customers
    print(f'  dim_customers: {len(customers):,} lignes')

    # Sellers
    sellers = duplicate_remove(dataframes['sellers'],'seller_id')
    result['dim_sellers'] = sellers
    print(f'  dim_sellers: {len(sellers):,} lignes')


    # Products avec traduction
    products = duplicate_remove(dataframes['products'],'product_id')
    translation = dataframes['translation']
    prproducts = products.merge(translation, on='product_category_name', how='left')
    result['dim_products'] = products
    print(f"   dim_products: {len(products):,} lignes (avec traduction)")


    # ===========================================
    # 3. Construction de la table de faits
    # ===========================================

    print('\n3 Construction la table de faits... ')

    fact = dataframes['order_items'].copy()
    orders = dataframes['orders'].copy()
    

    # Calcul du revenu total par item
    fact['item_total'] = fact['price'].fillna(0)
    if 'freight_value' in fact.columns:
        fact['item_total'] = fact['item_total'] + fact['freight_value'].fillna(0)

    
    # Jointure avec orders
    fact = fact.merge(orders, on = 'order_id')

    # Jointure avec customers
    fact = fact.merge(
        customers[['customer_id', 'customer_city', 'customer_state']],
        on = 'customer_id', how = 'left'
    )

    # Jointure avec product
    product_cols = ['product_id', 'product_category_name']
    if 'product_category_name_english' in products.columns:
        product_cols.append('product_category_name_english')
    fact = fact.merge(products[product_cols], on = 'product_id', how = 'left')


    result['fact_order_items'] = fact
    print(f' fact_order_items: {len(fact):,} lignes')

    # ==============================
    # 4. Calcul des metriques
    # ==============================
    print('\n4 Calcul des metriques... ')


    # Creation de la colonne year_month
    if 'order_purchase_timestamp' in fact.columns:
        fact['year_month'] = fact['order_purchase_timestamp'].dt.to_period('M').astype(str)






 





    
