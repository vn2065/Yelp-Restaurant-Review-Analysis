# INSERT INTO yelp.businesses JSON '{"Name": "Levain" Bakery","Cuisine": "mexican",  "Rating": "4.5", "Business ID": "H4jJ7XB3CetIr1pg56CczQ", "Zip Code": "10023", "Longitude": "-73.980299", "Latitude": "40.779961", "Address": "167 W 74th St New York, NY 10023", "Number of Reviews": 9304, "id": "H4jJ7XB3CetIr1pg56CczQ", "insertedAtTimestamp": "1669417772.7673270702362060546875"}';
# INSERT INTO yelp.businesses JSON '{ "\"Cuisine\"": "mexican"}';
# INSERT INTO yelp.businesses JSON ''{"Cuisine": "mexican2"}'';
INSERT INTO "yelp"."businesses" JSON '{"\"Cuisine\"": "mexican", "\"Name\"": "Levain Bakery", "\"Rating\"": "4.5", "\"Business ID\"": "H4jJ7XB3CetIr1pg56CczQ", "\"Zip Code\"": "10023", "\"Longitude\"": "-73.980299", "\"Latitude\"": "40.779961", "\"Address\"": "167 W 74th St New York, NY 10023", "\"Number of Reviews\"": "9304", "\"id\"": "H4jJ7XB3CetIr1pg56CczQ", "\"insertedAtTimestamp\"": "1669417772.7673270702362060546875"}';

INSERT INTO "yelp"."businesses" JSON '{"\"Cuisine\"": "mexican", "\"Name\"": "Katz's Delicatessen", "\"Rating\"": "4.0", "\"Business ID\"": "V7lXZKBDzScDeGB8JmnzSA", "\"Zip Code\"": "10002", "\"Longitude\"": "-73.9875259", "\"Latitude\"": "40.722237", "\"Address\"": "205 E Houston St New York, NY 10002", "\"Number of Reviews\"": "14354", "\"id\"": "V7lXZKBDzScDeGB8JmnzSA", "\"insertedAtTimestamp\"": "1669417772.80432796478271484375"}';

item = {
    'id': '1',
    'Business ID': '2',
    'insertedAtTimestamp': '3',
    'Name': '4',
    'Cuisine': '5',
    'Rating': '6',
    'Number of Reviews': '7',
    'Address': '8',
    'Zip Code': '9',
    'Latitude': '10',
    'Longitude': '11'
}
final = '{'
for key, value in item.items():
    final = final + '"\\"'+key+'\\""'+': '+'"'+value+'", '
final = final[:-2] + '}'
print(final)