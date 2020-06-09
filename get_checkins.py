import time
from datetime import datetime
from clickhouse_driver import Client
from clickhouse_driver import errors
import requests


client = Client(host='ec2-3-136-155-185.us-east-2.compute.amazonaws.com', user='default', password='', port='9000', database='default')
client_id = 'A08B11D992970D4FF58BAC219206B04E160E12FF'
client_secret = '750ADED063145180DC1085E657780CD0A56941EE'
count_of_inserted_beer_reviews = 0
count_of_inserted_users = 0
count_of_inserted_beers = 0
count_of_inserted_venues = 0
count_of_collected_checkins = 0
flatten = lambda lst: [item for sublist in lst for item in sublist]
ids = client.execute(f'SELECT brewery_id FROM brewery_info')
ids_list = flatten(ids)
for brewery_id in ids_list:
    max_id = 0
    while max_id != '':
        print('brewery_id:', brewery_id, 'collected:', count_of_collected_checkins)
        response = requests.get(f'https://api.untappd.com/v4/brewery/checkins/{brewery_id}?client_id={client_id}&client_secret={client_secret}',
                               params={
                                   'max_id': max_id
                               })
        item = response.json()
        count_of_collected_checkins += item['response']['checkins']['count']
        first_id = item['response']['checkins']['items'][0]['checkin_id']
        max_id = item['response']['pagination']['max_id']
        
        for checkin in item['response']['checkins']['items']:
            # beer_reviews
            checkin_id = checkin['checkin_id']
            if client.execute(f'SELECT count(1) FROM beer_reviews WHERE checkin_id={checkin_id}')[0][0] == 0:
                insert_into_beer_reviews = []
                insert_into_beer_reviews.append(checkin['checkin_id'])
                insert_into_beer_reviews.append(datetime.strptime(checkin['created_at'][5:-6], '%d %b %Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'))
                insert_into_beer_reviews.append(checkin['checkin_comment'].replace("'", "").replace('"', ''))
                insert_into_beer_reviews.append(checkin['rating_score'])
                insert_into_beer_reviews.append(checkin['user']['uid'])
                try:
                    insert_into_beer_reviews.append(checkin['venue']['venue_id'])
                except TypeError:
                    insert_into_beer_reviews.append('cast(Null as Nullable(UInt64))')
                insert_into_beer_reviews.append(checkin['beer']['bid'])
                insert_into_beer_reviews.append(checkin['brewery']['brewery_id'])
                print('beer_reviews:', insert_into_beer_reviews)
                for index, element in enumerate(insert_into_beer_reviews):
                    if element is None:
                        insert_into_beer_reviews[index] = ''
                try:
                    client.execute(f'INSERT INTO beer_reviews VALUES {tuple(insert_into_beer_reviews)}')
                except errors.ServerException as E:
                    print(E)
                count_of_inserted_beer_reviews += 1
                print('count_of_inserted_beer_reviews:', count_of_inserted_beer_reviews)
            # users
            user_id = checkin['user']['uid']
            if client.execute(f'SELECT count(1) FROM users WHERE user_id={user_id}')[0][0] == 0:
                insert_into_users = []
                insert_into_users.append(checkin['user']['uid'])
                insert_into_users.append(checkin['user']['user_name'])
                insert_into_users.append(checkin['user']['first_name'])
                insert_into_users.append(checkin['user']['last_name'])
                insert_into_users.append(checkin['user']['location'])
                insert_into_users.append(checkin['user']['url'])
                insert_into_users.append(checkin['user']['is_supporter'])
                insert_into_users.append(checkin['user']['bio'].replace("'", "").replace('"', ''))
                insert_into_users.append(checkin['user']['relationship'])
                insert_into_users.append(checkin['user']['user_avatar'])
                print('users:', insert_into_users)
                for index, element in enumerate(insert_into_users):
                    if element is None:
                        insert_into_users[index] = ''
                try:
                    client.execute(f'INSERT INTO users VALUES {tuple(insert_into_users)}')
                except errors.ServerException as E:
                    print(E)
                count_of_inserted_users += 1
                print('count_of_inserted_users:', count_of_inserted_users)
            # beers
            beer_id = checkin['beer']['bid']
            if client.execute(f'SELECT count(1) FROM beers WHERE beer_id={beer_id}')[0][0] == 0:
                insert_into_beers = []
                insert_into_beers.append(checkin['beer']['bid'])
                insert_into_beers.append(checkin['beer']['beer_name'].replace("'", "").replace('"', ''))
                insert_into_beers.append(checkin['beer']['beer_label'])
                insert_into_beers.append(checkin['beer']['beer_abv'])
                insert_into_beers.append(checkin['beer']['beer_ibu'])
                insert_into_beers.append(checkin['beer']['beer_slug'].replace("'", "").replace('"', ''))
                insert_into_beers.append(checkin['beer']['beer_description'].replace("'", "").replace('"', ''))
                insert_into_beers.append(checkin['beer']['beer_style'])
                insert_into_beers.append(int(checkin['beer']['has_had']))
                insert_into_beers.append(checkin['beer']['beer_active'])
                print('beers', insert_into_beers)
                for index, element in enumerate(insert_into_beers):
                    if element is None:
                        insert_into_beers[index] = ''
                try:
                    client.execute(f'INSERT INTO beers VALUES {tuple(insert_into_beers)}')
                except errors.ServerException as E:
                    print(E)
                count_of_inserted_beers += 1
                print('count_of_inserted_beers:', count_of_inserted_beers)
            # venues
            try:
                venue_id = checkin['venue']['venue_id']
                if client.execute(f'SELECT count(1) FROM venues WHERE venue_id={venue_id}')[0][0] == 0:
                    insert_into_venue = []
                    insert_into_venue.append(checkin['venue']['venue_id'])
                    insert_into_venue.append(checkin['venue']['venue_name'])
                    insert_into_venue.append(checkin['venue']['venue_slug'])
                    insert_into_venue.append(checkin['venue']['primary_category_key'])
                    insert_into_venue.append(checkin['venue']['primary_category'])
                    insert_into_venue.append(checkin['venue']['parent_category_id'])
                    insert_into_venue.append(checkin['venue']['categories']['items'][0]['category_key'])
                    insert_into_venue.append(checkin['venue']['categories']['items'][0]['category_name'])
                    insert_into_venue.append(checkin['venue']['categories']['items'][0]['category_id'])
                    insert_into_venue.append(int(checkin['venue']['categories']['items'][0]['is_primary']))
                    insert_into_venue.append(checkin['venue']['location']['venue_address'])
                    insert_into_venue.append(checkin['venue']['location']['venue_city'])
                    insert_into_venue.append(checkin['venue']['location']['venue_state'])
                    insert_into_venue.append(checkin['venue']['location']['venue_country'])
                    insert_into_venue.append(checkin['venue']['location']['lat'])
                    insert_into_venue.append(checkin['venue']['location']['lng'])
                    insert_into_venue.append(checkin['venue']['contact']['twitter'])
                    insert_into_venue.append(checkin['venue']['contact']['venue_url'])
                    insert_into_venue.append(checkin['venue']['foursquare']['foursquare_id'])
                    insert_into_venue.append(checkin['venue']['foursquare']['foursquare_url'])
                    insert_into_venue.append(checkin['venue']['venue_icon']['sm'])
                    insert_into_venue.append(checkin['venue']['venue_icon']['md'])
                    insert_into_venue.append(checkin['venue']['venue_icon']['lg'])
                    insert_into_venue.append(int(checkin['venue']['is_verified']))
                    for index, element in enumerate(insert_into_venue):
                        if element is None:
                            insert_into_venue[index] = ''
                    print('venues:', insert_into_venue)
                    try:
                        client.execute(f'INSERT INTO venues VALUES {tuple(insert_into_venue)}')
                    except errors.ServerException as E:
                        print(E)
                    count_of_inserted_venues += 1
                    print('count_of_inserted_venues:', count_of_inserted_venues)
            except TypeError:
                pass
        detailed_log_tuple = (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), brewery_id, count_of_collected_checkins, first_id, max_id)
        try:
            client.execute(f'INSERT INTO log_brewery_checkins_detailed VALUES {detailed_log_tuple}')
        except errors.ServerException as E:
            print(E)
        time.sleep(37)
        item = 0
    log_tuple = (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), brewery_id, count_of_collected_checkins)
    client.execute(f'INSERT INTO log_brewery_checkins VALUES {log_tuple}')
    count_of_collected_checkins = 0
