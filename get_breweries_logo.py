import requests
from clickhouse_driver import Client


client = Client(host='ec2-3-136-155-185.us-east-2.compute.amazonaws.com', user='default', password='', port='9000', database='default')
labels = client.execute('SELECT brewery_label FROM brewery_info')
ids = client.execute('SELECT brewery_id FROM brewery_info')
flatten = lambda lst: [item for sublist in lst for item in sublist]
labels = flatten(labels)
ids = flatten(ids)
for brewery_id, brewery_labes in zip(ids, labels):
    fname = brewery_id
    img_data = requests.get(brewery_labes).content
    with open(f'brewery_logo/{fname}', 'wb') as handler:
        handler.write(img_data)
