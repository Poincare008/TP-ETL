import pandas as pd 


def read_csv_file(file_name):
    """
        Read a csv file using pandas
        :param file_name: File name
    """
    df = pd.read_csv(f'C:/Users/hp/Documents/TP-ETL/tp_etl/sqlite_exports/{file_name}.csv', low_memory=False)
    df = df.drop(columns=['index'])
    
    # Affichage des statistiques de base sur le DataFrame
    print(f"====== Statistics on {file_name} =======")
   #affichage de la forme du DataFrame
    print(f"Dimension {df.shape}")
    # Affichage des types de données de chaque colonne ainsi que les premières lignes du DataFrame
    print(f"First rows in {file_name}")
    print(df.head(5))
    print(f"affichage Types on  {file_name}")
    print(df.dtypes)
    # Affichage des informations complètes sur le DataFrame
    print(f"affichage des informations sur  {file_name}")
    print(df.info())
    # Affichage des statistiques descriptives pour les colonnes numériques du DataFrame
    print(f"affichage des statistiques descriptives sur  {file_name}")
    print(df.describe())
    return df

def extract(): 
    df_customers = read_csv_file('customers')
    df_orders = read_csv_file('orders')
    df_geoloc = read_csv_file('geoloc')
    df_products = read_csv_file('products')
    df_sellers = read_csv_file('sellers')

    df_translations = read_csv_file('translation')
    df_order_items = read_csv_file('order_items')
    df_payments = read_csv_file('order_pymts')
    df_order_reviews = read_csv_file('order_reviews')

    
   # df_orders_customers = pd.merge(df_orders, df_customers, on='customer_id')
   #print(df_orders_customers.shape)

    

    #df_orders_payments = pd.merge(df_orders, df_payments,
                                  #left_on='order_id', 
                                  #right_on='order_id', how='outer', indicator=True)

    #print(df_orders_payments.shape)
    #print(df_orders_payments['_merge'].value_counts())
    #df_missing_payment = df_orders_payments[df_orders_payments['_merge'] == 'left_only']

    #print(df_missing_payment)

def change_date_format(df_order_items, df_order_reviews, df_orders):
   df_order_items["shipping_limit_date"] = pd.to_datetime(df_order_items["shipping_limit_date"])
   #Conversion avec format spécifique 
   df_order_items["shipping_limit_date"] = pd.to_datetime(df_order_items["shipping_limit_date"]) 
   df_order_reviews["review_answer_timestamp"] = pd.to_datetime(df_order_reviews["review_answer_timestamp"]) 
   df_order_reviews["review_creation_date"] = pd.to_datetime(df_order_reviews["review_creation_date"]) 
   df_orders["order_purchase_timestamp"] = pd.to_datetime(df_orders["order_purchase_timestamp"]) 
   df_orders["order_approved_at"] = pd.to_datetime(df_orders["order_approved_at"]) 
   df_orders["order_delivered_carrier_date"] = pd.to_datetime(df_orders["order_delivered_carrier_date"]) 
   df_orders["order_delivered_customer_date"] = pd.to_datetime(df_orders["order_delivered_customer_date"]) 
   df_orders["order_estimated_delivery_date"] = pd.to_datetime(df_orders["order_estimated_delivery_date"])
#verification du format de date après conversion
   print("Verification du format de date après conversion :")
   print(df_order_items["shipping_limit_date"].dtype)
   print(df_order_reviews["review_answer_timestamp"].dtype)
   print(df_order_reviews["review_creation_date"].dtype)
   print(df_orders["order_purchase_timestamp"].dtype)
   print(df_orders["order_approved_at"].dtype)
   print(df_orders["order_delivered_carrier_date"].dtype)
   print(df_orders["order_delivered_customer_date"].dtype)
   print(df_orders["order_estimated_delivery_date"].dtype)
   
   return df_order_items, df_order_reviews, df_orders

def duplicate_remove(df_customers, df_orders, df_geoloc, df_products, df_sellers, df_translations, df_order_items, df_payments, df_order_reviews):
    # Regrouper les DataFrames dans une liste
    dfs = [df_customers, df_orders, df_geoloc, df_products, df_sellers, df_translations, df_order_items, df_payments, df_order_reviews]
    
    # Supprimer les doublons pour chacun
    dfs = [df.drop_duplicates() for df in dfs]
    
    # Retourner le tuple comme avant
    return tuple(dfs)


def transform_data(df_customers, df_orders, df_geoloc, df_products, df_sellers, df_translations, df_order_items, df_payments, df_order_reviews):
    data = change_date_format(df_order_items, df_order_reviews, df_orders)
    data = duplicate_remove(df_customers, df_orders, df_geoloc, df_products, df_sellers, df_translations, data[0], df_payments, data[1])
    return data
    
    
 
#def load_data(data, output_file):
    #data.to_csv(output_file, index=False)
   
 
 
if __name__ == "__main__":
    extract()
    df_order_items = read_csv_file('order_items')
    df_order_reviews = read_csv_file('order_reviews')
    df_orders = read_csv_file('orders')
    df_customers = read_csv_file('customers')
    df_geoloc = read_csv_file('geoloc')
    df_products = read_csv_file('products')
    df_sellers = read_csv_file('sellers')
    df_translations = read_csv_file('translation')
    df_payments = read_csv_file('order_pymts')

    transform_data(df_customers, df_orders, df_geoloc, df_products, df_sellers, df_translations, df_order_items, df_payments, df_order_reviews)