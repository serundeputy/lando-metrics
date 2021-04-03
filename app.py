# app.py
''' Get, record, and measure growth of Lando metrics. '''

from datetime import datetime
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers, exceptions
import os
import pandas as pd

load_dotenv()
QBOX_USER = os.getenv('QBOX_USER')
QBOX_PASSWORD = os.getenv('QBOX_PASSWORD')

es = Elasticsearch([
        f'https://{QBOX_USER}:{QBOX_PASSWORD}@f505e785.qb0x.com:31626/'
    ])


def month_and_year(month):
    pieces = month.split(' ')
    return f'{pieces[1]} {pieces[2]}'


def get_recipe_metrics(start, end, recipe='lagoon'):
    ''' Get recipe metrics between start time and end time. '''
    start = int(datetime.strptime(start, '%d %B %Y').timestamp())
    end = int(datetime.strptime(end, '%d %B %Y').timestamp())
    doc = {
      'size': 500,
      'sort': [
        {
          'created': {
            'order': 'desc',
            'unmapped_type': 'boolean'
          }
        }
      ],
      'query': {
        'filtered': {
          'query': {
            'query_string': {
              'query': f'type: {recipe}'
            }
          },
          'filter': {
            'bool': {
              'must': [
                {
                  'range': {
                    'created': {
                      'gte': start*1000,
                      'lt': end*1000,
                    }
                  }
                }
              ],
              'must_not': []
            }
          }
        }
      },
      "aggs": {
        "2": {
          "date_histogram": {
            "field": "created",
            "interval": "12h",
            "pre_zone": "-04:00",
            "min_doc_count": 0,
            "extended_bounds": {
              "min": 1599008072083,
              "max": 1601514572084
            }
          }
        }
      },
      'fields': [
        '*',
        '_source'
      ],
      'script_fields': {},
      'fielddata_fields': [
        '@timestamp',
        'site.last_code_push.timestamp',
        'created',
        'last_code_push.timestamp'
      ]
    }
    if recipe == 'localdev':
        del doc['query']['filtered']['query']['query_string']['query']
        doc['query']['filtered']['query']['query_string']['query'] = f'product: {recipe}'

    res = es.search(
            index='metrics',
            body=doc,
            scroll='1s',
        )
    df = pd.json_normalize(res['hits']['hits'])
    sid = res['_scroll_id']
    scroll_size = len(res['hits']['hits'])

    while scroll_size > 0:
        "Scrolling..."
        res = es.scroll(
                body=doc,
                scroll_id=sid,
                scroll='2m'
            )
        sid = res['_scroll_id']
        df = df.append(pd.json_normalize(res['hits']['hits']))
        scroll_size = len(res['hits']['hits'])

    return df


months = [
    ['01 July 2020', '31 July 2020'],
    ['01 August 2020', '31 August 2020'],
    ['01 September 2020', '30 September 2020'],
    ['01 October 2020', '31 October 2020'],
    ['01 November 2020', '30 November 2020'],
    ['01 December 2020', '31 December 2020'],
    ['01 January 2021', '31 January 2021'],
    ['01 February 2021', '28 February 2021'],
    ['01 March 2021', '31 March 2021'],
]
providers = ['Pantheon', 'Localdev', 'PlatformSh', 'Lagoon']


def get_uniq_count(data):
    uniq_apps = data.groupby(['_source.app']).count()
    return len(uniq_apps)


for provider in providers:
    last_month = 0
    percent_growth = 0
    print(f'{provider}:')
    print('\t\t\tMonth\t\t Unique Apps\t\tGrowth')
    for month in months:
        metric = get_recipe_metrics(month[0], month[1], provider.lower())
        uniq_apps = get_uniq_count(metric)
        if last_month > 0:
            percent_growth = str(
                    round(
                        ((uniq_apps - last_month) / last_month) * 100
                    )
                ) + '%'
        else:
            percent_growth = 'NA'

        last_month = uniq_apps
        print(
            f'\t\t\t{month_and_year(month[0])}\t\t{uniq_apps}\t\t {percent_growth}'
        )
