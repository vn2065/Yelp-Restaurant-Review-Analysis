from kafka import KafkaConsumer
import json
import time
import cql
import tqdm
import logging
import re
# logging.basicConfig(level=logging.DEBUG)


def loadToCassandra():
    consumer = KafkaConsumer(
        'businesses',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    con = cql.connect(host='127.0.0.1', port=9160, cql_version='3.0.0')
    cursor = con.cursor()
    create_keyspace_cql = "CREATE KEYSPACE IF NOT EXISTS yelp  WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    drop_table_cql = 'DROP TABLE IF EXISTS "yelp"."reviews";'
    create_table_cql = 'CREATE TABLE IF NOT EXISTS "yelp"."reviews" (\
                        "Cuisine" TEXT,\
                        "Name" TEXT,\
                        "User ID" TEXT,\
                        "Business ID" TEXT,\
                        "Review" TEXT,\
                        "Rating" TEXT,\
                        "id" TEXT,\
                        "insertedAtTimestamp" TEXT PRIMARY KEY);'

    error_count = 0
    cursor.execute(create_keyspace_cql)
    cursor.execute(drop_table_cql)
    # time.sleep(5)
    cursor.execute(create_table_cql)
    # time.sleep(5)
    CQLString = 'INSERT INTO "yelp"."reviews" JSON '
    msg_cnt = 0

    for message in tqdm.tqdm(consumer):
        try:
            msg_cnt += 1
            message = message.value
            json_string = '{'
            for key, value in message.items():
                if isinstance(value, str):
                    value = value.encode('utf-8')
                else:
                    value = str(value)
                json_string = json_string + '"\\"' + key + '\\""' + ': ' + '"' + re.sub(r"([\'])", r'\\\\1', value) + '", '
            json_string = json_string[:-2] + '}'
            cql_insert_query = CQLString + "'" + json_string + "'" + ";"
            cursor.execute(bytes(cql_insert_query))
            print("Message Inserted: ", msg_cnt)

        except Exception as e:
            error_count += 1
    print('Errors: '+error_count)


loadToCassandra()
