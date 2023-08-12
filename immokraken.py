import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import os
import sqlalchemy
import pymysql
import math

# TODO cleaning, database ingestion, cluster analysis, word extraction

# directory = 'data'
engine = sqlalchemy.create_engine("mariadb+pymysql://root:root@192.168.178.40:3306/immokraken")

def get_match_num(soup):
    matches = soup.find("div", class_="title-a95df")
    
    match_text = soup.find("h1", class_="MatchNumber-a225f").get_text()
    
    match_num = re.search(r"(\d*)\.?(\d*)" ,match_text).group().replace(".","")
    
    match_num = math.ceil(int(match_num)/20)
    
    return match_num

def clean_listings_df(df):

    cols = list(df.columns)
    
    for col in cols:
        if df[col].dtype == object:
            df[col] = df[col].str.strip()

    df = df.replace('auf Anfrage', np.nan)
    df = df.replace(' ', np.nan)
    df = df.replace('', np.nan)
    
    if 'price' in cols:
        df['price'] = df['price'].replace(' €', '', regex=True).replace('\.', '', regex=True).replace('\,','.', regex=True).astype(float)
    
    if 'area' in cols:
        df['area'] = df['area'].replace('\sm.', '', regex=True).astype(float)
    
    if 'rooms' in cols:
        df['rooms'] = df['rooms'].replace('\sZi\.', '', regex=True).astype(float)
    
    separator = ', '
    
    if 'address' in cols:
        df['city'] = df['address'].apply(lambda x: x.split(separator, 1)[1] if separator in x else x)
        df['street'] = df['address'].apply(lambda x: x.split(separator, 1)[0] if separator in x else np.NaN)
        df['district'] = df['address'].str.extract(r'\((.*?)\)', expand=True)
        df['city'] = 'Berlin'
    
    df['created_at'] = pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'))
    
    return df

def main():

    page_index = 1

    url = "https://www.immowelt.de/liste/berlin/wohnungen/kaufen?d=true&sd=DESC&sf=TIMESTAMP&sp=" + str(page_index)
    page = requests.get(url)
    
    soup = BeautifulSoup(page.content, "html.parser")
    
    match_num = get_match_num(soup)
    
    # match_num = 308
    
    listings_list = []
    details_list = []
    bullets_list = []
    descriptions_list = []
    
    control_df = pd.read_sql('Select distinct identifier from immokraken.listings', con=engine)
    
    while page_index <= match_num:
    
        try:
            
            print("-----------------------")
            print(page_index)
            print("-----------------------")
        
            url = "https://www.immowelt.de/liste/berlin/wohnungen/kaufen?d=true&sd=DESC&sf=TIMESTAMP&sp=" + str(page_index)
            page = requests.get(url)
            
            soup = BeautifulSoup(page.content, "html.parser")

            listings = soup.find_all("div", class_="EstateItem-1c115")
            
            for listing in listings:
                
                # Crreate row dicts
                listings_row = {}
                details_row = {}
                bullets_row = {}
                descriptions_row = {}
                
                # get expose link
                link = listing.find("a", href=True)['href']
                print(link)
                identifier = re.search("(?<=expose/).*" , link)[0]
                
                if identifier in control_df['identifier'].values:
                    continue
                
                listings_row["identifier"] = identifier
                listings_row["link"] = link
                
                details_row["identifier"] = identifier
                bullets_row["identifier"] = identifier
                descriptions_row["identifier"] = identifier
                
                # extract main listing information
                title = listing.find("h2", class_=None)
                print(title.get_text())
                listings_row["title"] = title.get_text()
                
                price = listing.find("div", {'data-test':"price"})
                print(price.get_text())
                listings_row["price"] = price.get_text()
                
                area = listing.find("div", {'data-test':"area"})
                print(area.get_text())
                listings_row["area"] = area.get_text()
                
                rooms = listing.find("div", {'data-test':"rooms"})
                print(rooms.get_text())
                listings_row["rooms"] = rooms.get_text()
                
                icons = listing.find_all("div", class_="IconFact-e8a23")
                try:
                    address = icons[0].find("span", class_=None)
                    print(address.get_text())
                    listings_row["address"] = address.get_text()
                    
                except IndexError:
                    print("no address")
                    
                try:
                    attributes = icons[1].find("span", class_=None)
                    print(attributes.get_text())
                    listings_row["attributes"] = attributes.get_text()
                    
                except IndexError:
                    print("no attributes")
                    
                listings_list.append(listings_row)
                
                # get subpage html
                subpage = requests.get(link)
                subsoup = BeautifulSoup(subpage.content, "html.parser")
                
                equipment_section = subsoup.find("div", class_="equipment card-content ng-star-inserted")
                bullet_list = subsoup.find("div", class_="textlist textlist--icon card-content ng-star-inserted")
                energy_bullets = subsoup.find("div", class_="card__cell pb-100 pb-75:400")
                object_details = subsoup.find_all("div", class_="has-font-100")
                
                # get expose equipment items
                if equipment_section is not None:
                    equipment_names = equipment_section.find_all("p", class_="has-font-75 color-grey-500")
                    equipment_values = equipment_section.find_all("p", {'_ngcontent-sc222':"", 'class':""})
                    
                    for equipment_name, equipment_value in zip(equipment_names,equipment_values):
                        print(f"equipment: {equipment_name.get_text()}: {equipment_value.get_text()}")
                        details_row["name"] = equipment_name.get_text()
                        details_row["value"] = equipment_value.get_text()
                        details_list.append(details_row)
                        details_row = {"identifier": identifier}
                        
                # get expose energy details
                if energy_bullets is not None:
                    energy_names = energy_bullets.find_all("p", class_="color-grey-500 has-font-75")
                    energy_values = energy_bullets.find_all("p", {'_ngcontent-serverapp-c184':"", 'class':""})
                    
                    for energy_name, energy_value in zip(energy_names,energy_values):
                        print(f"energy bullet: {energy_name.get_text()}: {energy_value.get_text()}")
                        details_row["name"] = energy_name.get_text()
                        details_row["value"] = energy_value.get_text()
                        details_list.append(details_row)
                        details_row = {"identifier": identifier}
                        
                # get expose bullet points
                if bullet_list is not None:
                    for bullet_sublist in bullet_list:
                        bulletpoints = bullet_sublist.find_all("li")
                        for bulletpoint in bulletpoints:
                            print(f"bulletpoint: {bulletpoint.get_text()}")
                            bullets_row["value"] = bulletpoint.get_text()
                            bullets_list.append(bullets_row)
                            bullets_row = {"identifier": identifier}
                        
                # get different partial text elements
                if object_details is not None:
                    for object_detail in object_details:
                        print(f"object_detail: {object_detail.get_text()}")
                        descriptions_row["value"] = object_detail.get_text()
                        descriptions_list.append(descriptions_row)
                        descriptions_row = {"identifier": identifier}

                print()
                
            page_index += 1
        
        except ConnectionResetError:
            print()
            print(f"ConnectionResetError, trying again with page index {page_index}")
            page_index += 1
            continue
        except ConnectionError:
            print()
            print(f"ConnectionResetError, trying again with page index {page_index}")
            page_index += 1
            continue
        
    listings_df = pd.DataFrame(listings_list)
    listings_df_clean = clean_listings_df(listings_df)
    
    details_df = pd.DataFrame(details_list)
    bullets_df = pd.DataFrame(bullets_list)
    descriptions_df = pd.DataFrame(descriptions_list)
    
    print(listings_df.head())
    print()
    print(details_df.head())
    print()
    print(bullets_df.head())
    print()
    print(descriptions_df.head())
    print()
    
    # listings_df.to_csv(os.path.join(directory,'listings.csv'), sep='|')
    # details_df.to_csv(os.path.join(directory,'details.csv'), sep='|')
    # bullets_df.to_csv(os.path.join(directory,'bullets.csv'), sep='|')
    # descriptions_df.to_csv(os.path.join(directory,'descriptions.csv'), sep='|')
    
    listings_df_clean.to_sql(name='listings', con=engine, if_exists='append', index=False)
    details_df.to_sql(name='details', con=engine, if_exists='append', index=False)
    bullets_df.to_sql(name='bullets', con=engine, if_exists='append', index=False)
    descriptions_df.to_sql(name='descriptions', con=engine, if_exists='append', index=False)

if __name__ == '__main__':
    main()