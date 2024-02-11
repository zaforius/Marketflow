import pandas as pd
import re
from datetime import datetime
import os
from glob import glob
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import numpy as np

def transform_csv(input_file):
    # Read the CSV file into a DataFrame
    
    if 'v2' in input_file:
        df = pd.read_csv(input_file)
        def clean_and_convert_price(price):
            if price is not None:
                # Convert to string and remove euro sign
                cleaned_price = str(price).replace('.','').replace('€', '').replace(',','.')

                if cleaned_price.replace('.', '', 1).isdigit():
                # Convert to float
                    return float(cleaned_price)
                else:
                    return None
            
        df['final_price'] = df['final_price'].apply(clean_and_convert_price)
        df['price_before_discount'] = df['price_before_discount'].apply(clean_and_convert_price)
        df['price_of_weight'] = df['price_of_weight'].apply(clean_and_convert_price)
        df['price_of_weight_before_discount'] = df['price_of_weight_before_discount'].apply(clean_and_convert_price)
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')
        df['discount_amount'] = df['price_before_discount'] - df['final_price']
        df['discount_amount'] = df['discount_amount'].fillna(0)
        df = df.rename(columns={'dataset': 'store'})
    else: #Kritikos
        df = pd.read_csv(input_file, sep='~')
        
        def clean_and_convert_price_of_weight(price):
            numeric_pattern = r'(\d+\.\d+|\d+)'
            if pd.notna(price):
                # Extract the first numeric value from each cell
                match = re.search(numeric_pattern, price)
                if match:
                    price = match.group(0)
                
                price = price.replace('€', '')
                cleaned_price = float(price)
            return cleaned_price
                
        
        df['price_of_weight'] = df['price_of_weight'].apply(clean_and_convert_price_of_weight)
        df['final_price'] = df['final_price'].apply(clean_and_convert_price_of_weight)
        
        if df['start_price'].empty:
            df['price_before_discount'] = float('nan')
        else:
            df['price_before_discount'] = df['start_price'].str.replace('€ ','').astype('float')



        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        
        df['category'] = df['product_href'].str.split('/').str[2]
        df['SKU_number'] = df['product_href'].str.extract(r'(\d+)').astype('int64')
        
        df = df.rename(columns={'product_description': 'product_name', 'product_href': 'link'})

        # Drop 'Sub_Category' column
        df = df.drop('Sub_Category', axis=1, errors='ignore')

        # Create 'store' column
        df['store'] = 'https://kritikos-sm.gr/'  # Replace 'your_store_name' with the actual store name

        # If 'discount_amount' column doesn't exist, create it and set every row to NaN
        if 'discount_amount' not in df.columns:
            df['discount_amount'] = float('nan')

        # If 'price_before_discount' and 'price_of_weight_before_discount' columns don't exist, create them and set every row to NaN
        columns_to_create = ['price_before_discount', 'price_of_weight_before_discount']
        for col in columns_to_create:
            if col not in df.columns:
                df[col] = float('nan')

        # Reorder columns
        desired_order = ['SKU_number', 'product_name', 'category', 'final_price', 'price_before_discount',
                 'price_of_weight', 'price_of_weight_before_discount', 'link', 'date', 'store', 'discount_amount']
        df = df[desired_order]
            
    return df

def transformation_function():

    category = ['Φρούτα & Λαχανικά',
                'manabikh',
                'Φρέσκο Κρέας & Ψάρι',
                    'fresko-kreas', 
                    'galaktokomika',
                    'Γαλακτοκομικά & Είδη Ψυγείου',
                    'allantika',
                    'Τυριά, Αλλαντικά & Deli',
                    'turokomika',
                    'katapsuxh',
                    'Κατεψυγμένα Τρόφιμα',
                    'pantopwleio',
                    'Τρόφιμα',
                    'Αρτοζαχαροπλαστείο & Snacks',
                    'kaba',
                    'Μπύρες, Αναψυκτικά, Κρασιά & Ποτά',
                    'proswpikh-frontida',
                    'Προσωπική Φροντίδα',
                    'brefika',
                    'Φροντίδα για το Μωρό σας',
                    'kathariothta',
                    'Οικιακή Φροντίδα & Χαρτικά',
                    'pet-shop',
                    'Φροντίδα για το Κατοικίδιο σας'
    ]

    main_category = ['Μαναβική',
                    'Μαναβική',
                        'Κρέας και ψάρι',
                        'Κρέας και ψάρι',
                        'Γαλακτοκομικά & Είδη Ψυγείου',
                        'Γαλακτοκομικά & Είδη Ψυγείου',
                        'Τυριά, Αλλαντικά & Deli',
                        'Τυριά, Αλλαντικά & Deli',
                        'Τυριά, Αλλαντικά & Deli',
                        'Κατεψυγμένα Τρόφιμα',
                        'Κατεψυγμένα Τρόφιμα',
                        'Τρόφιμα',
                        'Τρόφιμα',
                        'Τρόφιμα',
                        'Είδη κάβας',
                        'Είδη κάβας',
                        'Προσωπική Φροντίδα',
                        'Προσωπική Φροντίδα',
                        'Βρεφικά',
                        'Βρεφικά',
                        'Οικιακή Φροντίδα & Χαρτικά',
                        'Οικιακή Φροντίδα & Χαρτικά',
                        'Είδη κατοικιδίου',
                        'Είδη κατοικιδίου'

    ]

    df_categories = pd.DataFrame({'category': category, 'main_category': main_category})


    # this part searches for the most recent csv file in the directory

    # Get the directory of the current script
    # script_dir = os.path.dirname(os.path.realpath(__file__))

    # Search for CSV files in the script directory
    # csv_files = glob(os.path.join(script_dir, '*.csv'))

    # kritikos_csvs = []
    # mymarket_csvs = []

    # split them in two lists according to origin

    # for file in csv_files:
    #     if 'www.mymarket.gr' in file:
    #         mymarket_csvs.append(file)
    #     else:
    #         kritikos_csvs.append(file)

    # Find the most recent CSV file based on modification time
    # kritikos_csv = max(kritikos_csvs, key=os.path.getmtime)
    # mymarket_csv = max(mymarket_csvs, key=os.path.getmtime)

    #product_results_v2.csv

    # apply the initial transformations

    dfk = transform_csv("product_results.csv")
    dfm = transform_csv("product_results_v2.csv")

    # unite the datasets
    df = pd.concat([dfk, dfm], axis=0, ignore_index=True)

    # make the dimension table 
    df_merged = pd.merge(df, df_categories, on='category', how='inner')
    df_dimension = df_merged[['SKU_number', 'category', 'main_category']]


    # make final transformation to drop duplicates
    df_dimension.drop_duplicates(subset='SKU_number', inplace=True)

    df_fact = df.drop('category', axis=1).drop_duplicates(subset='SKU_number')

    df_fact_to_export = df_fact#.head(100)
    df_dimension_to_export = df_dimension#.head(100)

    # Connect to Database and update Data
    
    conn = BaseHook.get_connection('postgres_ch')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    df_fact_to_export.to_sql(name='fact_table', con=engine, if_exists='append', index=False)
    df_dimension_to_export.to_sql(name='dimention_category', con=engine, if_exists='append', index=False)
    
    # Get current time for creating a unique file name
    # current_time = datetime.now().strftime("%Y%m%d%H%M%S")

    # Define the CSV file name
    # csv_filename1 = f"fact_{current_time}.csv"
    # csv_filename2 = f"dimension_{current_time}.csv"

    # Export DataFrame to CSV
    # df_fact_to_export.to_csv(csv_filename1, index=False, encoding='utf-8-sig')
    # df_dimension_to_export.to_csv(csv_filename2, index=False, encoding='utf-8-sig')

    # Print the file name
    # print(f"DataFrame exported to: {csv_filename1}")
    # print(f"DataFrame exported to: {csv_filename2}")

# transformation_function()