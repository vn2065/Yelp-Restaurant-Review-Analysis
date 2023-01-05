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
    search_api_url = 'https://api.yelp.com/v3/businesses/search'

    cities = {'New York'}
    cuisines = {'indian', 'chinese', 'mexican', 'italian',
                'thai', 'american', 'caribbean', 'korean'}

    try:
        for city in cities:
            for cuisine in tqdm.tqdm(cuisines):
                for i in range(0, 1000, 50):
                    params = {'term': cuisine + " restaurants",
                              'location': city,
                              'offset': i,
                              'limit': 50}

                    # url = "https://api.yelp.com/v3/businesses/search?location='New York'&limit=50&offset="+str(i)
                    #
                    # payload = {}
                    # headers = {
                    #     'Authorization': 'Bearer HYuY3DtdCvYy3WK0LliPxVXKxt63be-7_5q8EagWxoooYhB32IYL5yrNOswtdwYGtnroSEn3J0DBHFZUP0RR2_SCMqWm_vCnphDjIKIqHHf20Iy_2kHEUJRd2yd8Y3Yx'
                    # }
                    #
                    # response = requests.request("GET", url, headers=headers, data=payload)
                    response = requests.get(search_api_url, headers=headers, params=params, timeout=5)
                    data_dict = response.json(parse_float=Decimal)
                    # print(data_dict)
                    for business in data_dict['businesses']:
                        display_address = business['location']['display_address']
                        address_string = ' '.join(display_address)
                        item = {
                            'id': business['id'],
                            'Business ID': business['id'],
                            'insertedAtTimestamp': Decimal(time.time()),
                            'Name': business['name'],
                            'Cuisine': cuisine,
                            'Rating': business['rating'],
                            'Number of Reviews': business['review_count'],
                            'Address': address_string,
                            'Zip Code': business['location']['zip_code'],
                            'Latitude': str(business['coordinates']['latitude']),
                            'Longitude': str(business['coordinates']['longitude']),
                        }
                        producer.send('businesses', value=item)
                        producer.flush(timeout=100)
    except Exception as e:
        print(e)


yelp()
# time.sleep(100)