import pandas as pd
import csv
from bs4 import BeautifulSoup
from datetime import datetime
import time
import requests

#definition of classes

class WebPage:
    def __init__(self, url):
        self.url = url
        self.pages = 1
        self.content = []
        self.card_list = []
    
    def get_html_content(self):
        for page in range(1, self.pages+1):
        

            headers = {"user-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
            response = requests.get(self.url + f'?page={page}&perPage=96', headers=headers)
            
            temp_content = response.content
            self.content.append(temp_content)
                

        return self.content
    

    # this function to set the count of all pages
    def set_pages(self):
        self.pages = 1
        self.get_html_content()
        soup = BeautifulSoup(self.content[0], 'html.parser')

        try:
            page_con = soup.find('span', 'relative z-0 inline-flex rounded-md space-x-2').find_all('a')
            self.pages = int(page_con[-2].getText().strip().replace('\\n', ''))
        except:
            self.pages = 1
    
    # this function to extract the html for every page in the content list and make it a soup item
    def get_the_product_cards(self):
        
        for page in self.content:
            soup = BeautifulSoup(page, 'html.parser')
            init_list = soup.find_all('div', class_='w-full flex')
            
            self.card_list.extend(init_list)
            

    # take a (soup) card and query it to find the important fields, then insert them in a dictionary
    def get_dict(self, card):
        prod_data = {}
        
        prod_data['SKU_number'] = int(card.find('div', class_='sku').text.replace('Κωδ: ', '')) #done
        prod_data['product_name'] = card.find('h3').text.strip() #done
        prod_data['category'] = card.get('data-google-analytics-item-list-name') # done


        P = bool(card.find('span', class_='price'))
        LP = bool(card.find('span', class_='list-price'))
        B = bool(card.find('span', class_='font-bold'))
        BS = bool(card.find('span', class_='font-bold line-through'))

        identifier = (P << 3) | (LP << 2) | (B << 1) | BS

        if P:
            prod_data['final_price'] = card.find('span', class_='price').text.strip().replace('\\n','')
        else:
            prod_data['final_price'] = None

        if LP:
            prod_data['price_before_discount'] = card.find('span', class_='list-price').text.strip().replace('\\n','')
        else:
            prod_data['price_before_discount'] =  None

        if B:
            prod_data['price_of_weight'] = card.find('span', class_='font-bold').text.strip().replace('\\n','')
        else:
            prod_data['price_of_weight'] = None

        if BS:
            prod_data['price_of_weight_before_discount'] = card.find('span', class_='font-bold line-through').text.strip().replace('\\n','')
        else:
            prod_data['price_of_weight_before_discount'] = None


        if identifier == 5:
            prod_data['final_price'] = prod_data['price_before_discount']
            prod_data['price_of_weight'] = prod_data['price_of_weight_before_discount']
        if identifier == 4:
            prod_data['final_price'] = prod_data['price_before_discount']
        if identifier == 1:
            prod_data['price_of_weight'] = prod_data['price_of_weight_before_discount']
        if identifier == 7:
            prod_data['final_price'] = prod_data['price_before_discount']
        if identifier == 9 or identifier == 13:
            prod_data['price_of_weight'] == prod_data['price_of_weight_before_discount']


        prod_data['link'] = card.find('h3').find('a').get('href')
        current_date_time = datetime.now()

        # Format the date as a string
        formatted_date = current_date_time.strftime("%Y-%m-%d %H:%M:%S")
        prod_data['date'] = formatted_date
        prod_data['dataset'] = self.url.split('/')[2]

        return prod_data
        
    # take all the product cards and extract the dictionaries
    def get_list(self):
        return list(map(self.get_dict, self.card_list))
    

def extract_and_save_to_csv(data_list):
    if not data_list:
        print("Error: The data list is empty.")
        return

    
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # Generate the CSV file name
    # csv_file_name = f"product_results_{current_datetime}_{data_list[0]['dataset']}.csv"
    csv_file_name = "product_results_v2.csv"
    try:
        # Write the list of dictionaries to the CSV file
        with open(csv_file_name, 'w', newline='', encoding='utf-8-sig') as csvfile:
            print('Scraping complete, extracting csv...', end='\r')
            fieldnames = data_list[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the header
            writer.writeheader()

            # Write the data
            writer.writerows(data_list)

        print(f"CSV file '{csv_file_name}' created successfully.")
    except Exception as e:
        print(f"Error: {e}")


def extraction_function():

    # the page list of the source
    page_list = [

        'https://www.mymarket.gr/offers/kalathi-toy-noikokyrioy',
        'https://www.mymarket.gr/frouta-lachanika',
        'https://www.mymarket.gr/fresko-kreas-psari',
        'https://www.mymarket.gr/galaktokomika-eidi-psygeiou',
        'https://www.mymarket.gr/tyria-allantika-deli',
        'https://www.mymarket.gr/katepsygmena-trofima',
        'https://www.mymarket.gr/mpyres-anapsyktika-krasia-pota',
        'https://www.mymarket.gr/proino-rofimata-kafes',
        'https://www.mymarket.gr/proino-rofimata-kafes',
        'https://www.mymarket.gr/artozacharoplasteio-snacks',
        'https://www.mymarket.gr/trofima',
        'https://www.mymarket.gr/frontida-gia-to-moro-sas',
        'https://www.mymarket.gr/prosopiki-frontida',
        'https://www.mymarket.gr/oikiaki-frontida-chartika',
        'https://www.mymarket.gr/kouzina-mikrosyskeves-spiti',
        'https://www.mymarket.gr/frontida-gia-to-katoikidio-sas',
        'https://www.mymarket.gr/epochiaka'
    ]

    final_list = []
    print('Scraping, please wait...this might take a couple of minutes')
    v = 0
    for page in page_list:
        #print('||'*(v), end='\r')
        #v += 1
        print(page)
        mymarket = WebPage(page)
        mymarket.set_pages()
        mymarket.get_html_content()
        mymarket.get_the_product_cards()
        final_list.extend(mymarket.get_list())
        print("done")



    extract_and_save_to_csv(final_list)

#extraction_function()
