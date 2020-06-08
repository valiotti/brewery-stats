import requests
import pandas as pd
import time
from clickhouse_driver import Client


client = Client(host='ec2-3-136-155-185.us-east-2.compute.amazonaws.com', user='default', password='', port='9000', database='default')
client_id = 'A08B11D992970D4FF58BAC219206B04E160E12FF'
client_secret = '750ADED063145180DC1085E657780CD0A56941EE'

df = pd.read_csv('brewery_data.csv', names=['brewery_id', 'beer_count', 'brewery_name', 'brewery_slug', 'brewery_url', 'city', 'lat', 'lng'])
brewery_ids = list(df['brewery_id'])

count = 0

for brewery_id in brewery_ids:
    if client.execute(f'SELECT count(1) FROM brewery_info WHERE brewery_id={brewery_id}'):
        print('ID:', count)
        print('Осталось:', len(brewery_ids) - count)
        response = requests.get(f'https://api.untappd.com/v4/brewery/info/{brewery_id}?client_id={client_id}&client_secret={client_secret}')
        item = response.json()
        data_to_insert_in_brewery_info = []
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_id'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_name'].replace("'", "").replace("`", ""))
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_slug'].replace("'", "").replace("`", ""))
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_label'].replace("'", "").replace("`", ""))
        data_to_insert_in_brewery_info.append(item['response']['brewery']['country_name'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_in_production'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['is_independent'])
        data_to_insert_in_brewery_info.append(int(item['response']['brewery']['claimed_status']['is_claimed']))
        data_to_insert_in_brewery_info.append(item['response']['brewery']['claimed_status']['follower_count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['claimed_status']['uid'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['beer_count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['contact']['twitter'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['contact']['facebook'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['contact']['instagram'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['contact']['url'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_type'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_type_id'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['location']['brewery_address'])
        data_to_insert_in_brewery_info.append((item['response']['brewery']['location']['brewery_city']).replace("'", "").replace("`", ""))
        data_to_insert_in_brewery_info.append(item['response']['brewery']['location']['brewery_state'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['location']['brewery_lat'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['location']['brewery_lng'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['rating']['count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['rating']['rating_score'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['brewery_description'].replace("'", "").replace("`", ""))
        data_to_insert_in_brewery_info.append(item['response']['brewery']['stats']['total_count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['stats']['unique_count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['stats']['monthly_count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['stats']['weekly_count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['stats']['user_count'])
        data_to_insert_in_brewery_info.append(item['response']['brewery']['stats']['age_on_service'])
        for index, item in enumerate(data_to_insert_in_brewery_info):
            if item is None:
                data_to_insert_in_brewery_info[index] = ''
        item = 0
        client.execute(f'INSERT INTO brewery_info VALUES {tuple(data_to_insert_in_brewery_info)}')
        count+= 1
        time.sleep(37)
