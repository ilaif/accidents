import googlemaps
import json

gmaps = googlemaps.Client(key=open('../google-api-key.txt').read())

cities = json.load(open('cities.json', 'r'))

cities_geo = {}
for city in cities:
    res = gmaps.geocode(city)
    if len(res) > 0:
        cities_geo[city] = res[0]['geometry']['location']
    else:
        cities_geo[city] = None
        print('WARNING: no result found for ' + city)

json.dump(cities_geo, open('cities_geo.json', 'w'))
