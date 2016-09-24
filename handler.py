#!/usr/bin/env python
import csv
import itertools
import requests

# unicode csv reading
def unicode_csv_dict_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.DictReader(utf_8_encoder(unicode_csv_data))
    for d in csv_reader:
        yield {k: unicode(d[k], 'utf-8') if type(d[k]) is str else d[k]
               for k in d}
def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')
def get_nth_from_iterable(n, iterable):
    return next(itertools.islice(iterable, n, n+1))

# misc parsing of tagme response
def merge_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z
def json_foolproof_loads(text):
    try:
        return json.loads(text)
    except:
        return {}

# TAGME setup
TAGME_KEY = open('.tagme.txt', 'r').read().strip()
TAGME_URL = "https://tagme.d4science.org/tagme/tag"

# TAGME
# Usage: tagme_tag(text)
tagme_payload = lambda text: {
    'text': text.encode('utf-8'),
    'lang': 'en',
    'include_categories': 'true',
    'include_all_spots': 'true',
    'long_text': 0,
    'epsilon': 0.1,
    'gcube-token': TAGME_KEY
}
tagme_call = lambda text: requests.get(TAGME_URL.strip(), params=tagme_payload(text))
tagme_tag = lambda text: merge_dicts(json_foolproof_loads(tagme_call(text.strip()).text), {u'raw': text})

def hello(event, context):
    csvfile = open('messages.csv')
    reader = unicode_csv_dict_reader(csvfile)
    msg = get_nth_from_iterable(0, reader)
    return msg
