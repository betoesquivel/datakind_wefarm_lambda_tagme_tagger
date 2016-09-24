#!/usr/bin/env python
import csv
import itertools
import requests
import json
import random

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

def tag_nth_message(n):
    csvfile = open('messages.csv', 'r')
    reader = unicode_csv_dict_reader(csvfile)
    msg = get_nth_from_iterable(n, reader)
    return tagme_tag(msg['body'])

def tag_nth_message_in_reader(n, reader):
    msg = get_nth_from_iterable(n, reader)
    return [msg, tagme_tag(msg['body'])]

def accum(vals, val):
    if vals:
        val -= (sum(map(lambda i: i + 1, vals)))
    vals.append(val)
    return vals

# shuffle annotations before dynamo upload
def roundrobin(*iterables):
    from itertools import cycle, islice
    pending = len(iterables)
    nexts = cycle(iter(it).next for it in iterables)
    while pending:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))

def merge_annotation_with_msg_fields(a, msg):
    return merge_dicts(a, {'full_text': msg['body'], 'message_id': msg['message_id']})

def hello(event, context):
    jobs = event.get('n', [1])
    jobs.sort()
    csvfile = open('messages.csv', 'r')
    reader = unicode_csv_dict_reader(csvfile)

    jobs_accum = reduce(accum, jobs, [])

    msgs, tagged_msgs = zip(*map(lambda j: tag_nth_message_in_reader(j, reader), jobs_accum))
    annotations_per_msg = [map(lambda a: merge_annotation_with_msg_fields(a, msg),
                               tagged['annotations'])
                           for msg, tagged in zip(msgs, tagged_msgs)]

    # Shuffle before writing to dynamo
    shuffled_annotations = list(roundrobin(*annotations_per_msg))
    shuffled_msgs = list(msgs)
    random.shuffle(shuffled_msgs)

    print type(shuffled_msgs[0]['message_id'])
    print type(shuffled_annotations[0]['start'])
    return [shuffled_annotations, shuffled_msgs]
