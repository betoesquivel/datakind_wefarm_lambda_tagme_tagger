#!/usr/bin/env python
import csv
import itertools
import requests
import json
import random
from pprint import PrettyPrinter
import uuid

pp = PrettyPrinter()

import boto3


#SETTING UP THE QUEUE
AWS_SQS_REGION = 'eu-west-1'
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='tag_msg_jobs')


# get db resource
def get_resource():
    resource = boto3.resource('dynamodb')
    return resource
dynamodb = get_resource()

# get tables
def get_table(table_name):
    table_names = [t.name for t in dynamodb.tables.all()]
    if table_name in table_names:
        return dynamodb.Table(table_name)
    else:
        print("Table name doesn't exist")
        return None
annotations_table = get_table('datadive_wefarm_annotations')
messages_table = get_table('datadive_wefarm_messages')

# batch put items in their respective dynamo table
def batch_put_items_in_table(items, table):
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

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
    categories = '|'.join(a['dbpedia_categories']) or "none"

    return merge_dicts(a, {'full_text': msg['body'],
                           'message_id': msg['message_id'],
                           'dbpedia_categories': categories,
                           'uuid': str(uuid.uuid1())})


def hello(event, context):
    # read the jobs from sqs
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

    print "Putting messages in table"
    batch_put_items_in_table(shuffled_msgs, messages_table)
    print "Finished with the  messages in table"

    print "Putting annotations in table"
    batch_put_items_in_table(shuffled_annotations, annotations_table)
    print "Finished with the annotations in table"

    # delete the jobs from sqs

    return [shuffled_annotations, shuffled_msgs]

def process(event, context):
    queue_jobs = queue.receive_messages(
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1
    )
    print "{} jobs read".format(len(queue_jobs))
    jobs = [int(job.body) for job in queue_jobs]

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

    print "Putting messages in table"
    batch_put_items_in_table(shuffled_msgs, messages_table)
    print "Finished with the  messages in table"

    print "Putting annotations in table"
    batch_put_items_in_table(shuffled_annotations, annotations_table)
    print "Finished with the annotations in table"

    print "{} jobs proc".format(len(queue_jobs))
    # delete the jobs from sqs
    for job in queue_jobs:
        job.delete()
    print "{} jobs left".format(len(queue_jobs))

    return [shuffled_annotations, shuffled_msgs]
