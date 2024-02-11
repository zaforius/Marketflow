import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from URLs import url_list
from datetime import date
import numpy as np

def extract_function():

    today = date.today()
    #date = pd.to_datetime('today')
    #string_date = date.strftime("%d-%m-%Y, %H.%M")

    list_of_products = []

    for url in url_list:
        new_response = requests.get(url)
        print(f"{url} with response {new_response.status_code}")
        new_soup = BeautifulSoup(new_response.content, 'html.parser')
        
        ###### Find Container that holds all Products
        try:
            external_container = new_soup.find('div',class_ = 'products_productScrollingContainer__1CZkB')
        except:
            external_container = np.nan

        ###### Find SubCategory Title
        try:
            Sub_Category_title = external_container.find('h1',class_='ProductMenu_listTitle__PxrUW').text
        except:
            Sub_Category_title = np.nan    
        ###### Products
        try:
            product_containers = external_container.find_all('div', class_ = 'ProductListItem_productItem__cKUyG')
            print(f"product containers: {len(product_containers)}")

            for product in product_containers:
                try:
                    product_description = product.find('p',class_='ProductListItem_title__e6MEz').text
                except:
                    product_description= np.nan
                try:
                    price_of_weight = product.find('p',class_='ProductListItem_description__DRAGa').text
                except:
                    price_of_weight = np.nan
                try:
                    final_price = product.find('p', class_ = "ProductListItem_finalPrice__sEMjs").text
                except:
                    final_price = np.nan
                try:
                    product_href = product.find('a',class_ = 'ProductListItem_productLink__BZo3P').get('href')
                except:
                    product_href = np.nan
                try:
                    start_price = product.find('p', class_ = "ProductListItem_beginPrice__vK_Dk").text
                except:
                    start_price = np.nan
                
                product_dict = {}
                product_dict['product_description'] = product_description
                product_dict['price_of_weight'] = price_of_weight
                product_dict['final_price'] = final_price
                product_dict['start_price'] = start_price
                product_dict['product_href'] = product_href
                product_dict['Sub_Category'] = Sub_Category_title
                product_dict['date'] = today.strftime("%d/%m/%Y")

                list_of_products.append(product_dict)
        except:
            print("no products")
        #time.sleep(2)


    df = pd.DataFrame(list_of_products)

    # df.to_csv(f'product_results{string_date}.csv', sep = "~", encoding="UTF-8", index=False)
    df.to_csv('product_results.csv', sep = "~", encoding="UTF-8", index=False)

# extract_function()