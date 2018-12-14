import json
from pprint import pprint
import googlemaps
from datetime import datetime
import config

def parse(filename):

    with open(filename) as f:
        gmaps = googlemaps.Client(key=config.API_KEY)
        data = json.load(f)
        for d in data:
            lookup(d["institution"], gmaps)

def lookup(name, gmaps):

    # name = 'California Polytechnic State University-San Luis Obispo'
    # Geocoding an address
    # geocode_result = gmaps.geocode('1600 Amphitheatre Parkway, Mountain View, CA')
    result = gmaps.places(name)
    gmaps.place(result['place_id'],

            'ChIJN1t_tDeuEmsRUsoyG83frY4',
                          fields=['geometry', 'id'], language=self.language)

def main():
    parse('us_institutions.json')

if __name__ == '__main__':
    main()
# {u'status': u'OK', u'html_attributions': [], u'results': [{u'rating': 4.7, u'name': u'California Polytechnic State University', u'reference': u'ChIJUTVMBbTx7IARA5HZKYq0s5g', u'geometry': {u'location': {u'lat': 35.3050053, u'lng': -120.6624942}, u'viewport': {u'northeast': {u'lat': 35.30947355000001, u'lng': -120.64296305}, u'southwest': {u'lat': 35.29160055, u'lng': -120.68381085}}}, u'place_id': u'ChIJUTVMBbTx7IARA5HZKYq0s5g', u'plus_code': {u'global_code': u'847X884Q+22', u'compound_code': u'884Q+22 San Luis Obispo, California'}, u'photos': [{u'photo_reference': u'CmRaAAAArlPSQuPfI_zKemFqVOQvATlmsCTb6S7anlMFl3ruS5-xqLoGpwV-yx552qlVDe6AxAw8a5_7CCnf_jW5nYECwXktutX8qVRIflr4rC81KzIayTkDcEoTnUaV_H1NigGXEhA6L-6LJIqP1SJJWMiOwTe9GhQR9PtaocAnOdFjXfGJ0ovHDLNtzQ', u'width': 4032, u'html_attributions': [u'<a href="https://maps.google.com/maps/contrib/103241999295486768102/photos">Manu Molina</a>'], u'height': 3024}], u'formatted_address': u'San Luis Obispo, CA 93407, USA', u'id': u'b4743e60f2c945a245fbf15f074e4f1a64559449', u'types': [u'university', u'point_of_interest', u'establishment'], u'icon': u'https://maps.gstatic.com/mapfiles/place_api/icons/school-71.png'}]}
