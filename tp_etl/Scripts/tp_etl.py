import os
# import argparse
import pandas as pd
import sqlite3


# Dossier des CSV
#DATA_DIR = 'sqlite_exports'  
# Dossier de sortie
OUTPUT_DIR = 'outputs'       
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
    else:
        print("Il n'y a pas de doublons")

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
    products = products.merge(translation, on='product_category_name', how='left')
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

        # Revenu mensuel
        monthly_revenue = fact.groupby('year_month')['item_total'].sum().reset_index()
        monthly_revenue.columns = ['year_month','revenue']
        monthly_revenue = monthly_revenue.sort_values('year_month')
        result['monthly_revenue'] = monthly_revenue
        print(f'monthly_revenue: {len(monthly_revenue)} mois')

        # Top Categories
        category_col = 'product_category_name_english' if 'product_category_name_english' in fact.columns else 'product_category_name'
        if category_col in fact.columns:
            top_categories = fact.groupby(category_col)['item_total'].sum().reset_index()
            top_categories.columns = ['product_category', 'revenue']
            top_categories = top_categories.sort_values('revenue',ascending=False).head(10)
            result['top_categories'] = top_categories
            print(f'top_categories: {len(top_categories)} 10 catégories')


        # Delais de livraison
        if 'order_delivered_customer_date' in fact.columns:
            fact_delivery = fact[fact['order_delivered_customer_date'].notna()].copy()
            fact_delivery['delivery_days'] = (
                fact_delivery['order_delivered_customer_date'] - 
                fact_delivery['order_purchase_timestamp']
            ).dt.total_seconds() / (24*3600)


        # Filtrage des valeurs aberrantes
        fact_delivery = fact_delivery[
            (fact_delivery['delivery_days'] >= 0) &
            (fact_delivery['delivery_days'] <= 365)
        ]


        delivery_metrics = fact_delivery.groupby('year_month')['delivery_days'].mean().reset_index()
        delivery_metrics.columns = ['year_month', 'avg_delivery_days']
        result['delivery_metrics'] = delivery_metrics
        print(f' delivery_metrics: {len(delivery_metrics)} mois')


        # Review
        reviews = dataframes['order_reviews'].copy()
        reviews = reviews.merge(orders[['order_id', 'order_purchase_timestamp']], on='order_id', how='left')
        reviews['year_month'] = reviews['order_purchase_timestamp'].dt.to_period('M').astype(str)
        reviews_monthly = reviews.groupby('year_month')['review_score'].mean().reset_index()
        reviews_monthly.columns = ['year_month','avg_review_score']
        result['reviews_monthly'] = reviews_monthly
        print(f' reviews_monthly (Bonus): {len(reviews_monthly)} mois')


        return result





#============================================================================================
# Load & exporter les resultats
#============================================================================================

# from pickle import load


def load_data(transformed_data):
    """
     load: esporter en CSV et SQLite
    """
    print("\n" + "=" * 60)
    print("etap 3; Load  export  des donnees")
    print("=" * 60)

    # Creons le dossier de sortie
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Export csv
    print(f" \n1.export CSV dans {OUTPUT_DIR}/")
    for name, df in transformed_data.items():
        filepath= os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(filepath, index= False)
        print(f"    {name}.csv ({len(df):,}lignes)")

    # Expport SQLITE
    sqlite_path= os.path.join(OUTPUT_DIR, "etl.db")
    print(f"\n2. Export Sqlite: {sqlite_path}")

    conn = sqlite3.connect(sqlite_path)
    for name, df in transformed_data.items():
        df.to_sql(name, conn, if_exists="replace", index= False)
        print(f"    Table{name} ({len(df):,} lignes)")
    conn.close()

    print(f"\n Base Sqlite cree: {sqlite_path}")


def generer_rapport (dataframes , transformed_data):
    """    
     Generer un rapport simple
    """
    print("\n" + "="* 60)
    print(" geration du rapporta")
    print("=" * 60)

    rapport= []
    rapport.append("=" * 70)
    rapport.append("Rapport ETL_Tp")
    rapport.append("=" * 70)
    rapport.append ("")

    # donnees sources
    rapport.append(" donneees sources")
    rapport.append("-" * 70)
    for name, df in dataframes.items():
        rapport.append(f"\n{name}:")
        rapport.append(f"    - Lignes: {len(df):,}")
        rapport.append(f"    - Colonnes: {len(df.columns)}")
        rapport.append(f"    - Valeurs manquantes: {df.isna().sum().sum():,}")
                                        
    # donnees transformees
    rapport.append("\n\n Donnees transformees")
    rapport.append("-" * 70)
    for name, df in transformed_data.items():
        rapport.append(f"\n({name}: {len(df):,} lignes")

        if name == "monthly_revenue":
            rapport.append(f"    - Total revenue: {df['revenue'].sum():,.2f}")
        elif name == "top_categories":
            rapport.append(f"  → Top 1: {df.iloc[0]['product_category']}")
        elif name == "delivery_metrics":
            rapport.append(f"    - Delai moyen: {df['avg_delivery_days'].mean():.1f} jours")
                                                
                         
          
         

        
    # rapport.append(f"    - Categories uniques: {df['category'].nunique()}")

    rapport.append(f"\n" + "=" * 70)
            
    # Ecrire le rapport
    rapport_path= os.path.join(OUTPUT_DIR, "rapport_etl.txt")
    with open(rapport_path, "w" , encoding="utf-8") as f:
        f.write("\n".join(rapport))
        print(f"Rapport generer: {rapport_path}")
          

#=========================================================================
# min  fonction principale
#========================================================================= 
def main():
    """
    Orchestre tout le processus ETL
    """
    print("\n" + "=" * 60)
    print("* TP ETL -PYTHON+Pandas".center)
    print("*" * 55 + "\n")

    # Etape 1: Extraction
    dataframes= extract()

    #Etape 2:  Transformation
    transformed_data= transform_data(dataframes)

    # Etape 3: Load
    load_data(transformed_data)


    # Generer le rapport
    generer_rapport(dataframes, transformed_data)


    # Resume final
    print("=" * 60)
    print("Traitement ETL termine avec succes!")
    print("=" * 60)
    print(f"    Resultats dans: {OUTPUT_DIR}/")
    print(f"    {len(dataframes)} fichiers sources")
    print(f"    {len(transformed_data)} tables creees")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()










 





    
