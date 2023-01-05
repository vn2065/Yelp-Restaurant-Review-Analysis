import os
import time
import tqdm
import json
import requests
from kafka import KafkaProducer

from decimal import Decimal
import logging
# logging.basicConfig(level=logging.DEBUG)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def yelp():
    api_key = "HYuY3DtdCvYy3WK0LliPxVXKxt63be-7_5q8EagWxoooYhB32IYL5yrNOswtdwYGtnroSEn3J0DBHFZUP0RR2_SCMqWm_vCnphDjIKIqHHf20Iy_2kHEUJRd2yd8Y3Yx"
    producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                             value_serializer=lambda x:
                             json.dumps(x, cls=DecimalEncoder).encode('utf-8'))

    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    search_api_url = 'https://api.yelp.com/v3/reviews/search'

    cities = {'New York'}
    businesses = os.environ['businesses']

    try:
        for city in cities:
            for cuisine in tqdm.tqdm(businesses):
                for i in range(0, 1000, 50):
                    params = {'term': cuisine + " restaurants",
                              'location': city,
                              'offset': i,
                              'limit': 50}

                    response = requests.get(search_api_url, headers=headers, params=params, timeout=5)
                    data_dict = response.json(parse_float=Decimal)
                    # print(data_dict)
                    for business in data_dict['businesses']:
                        display_address = business['location']['display_address']
                        address_string = ' '.join(display_address)
                        item = {
                            'Cuisine': business['Cuisine'],
                            'Name': business['Name'],
                            'User ID': business['User ID'],
                            'Business ID': business['Business ID'],
                            'Review': business['Review'],
                            'Rating': business['Rating'],
                            'id': business['ID'],
                        }
                        producer.send('reviews', value=item)
                        producer.flush(timeout=100)
    except Exception as e:
        print(e)


yelp()
# time.sleep(100)