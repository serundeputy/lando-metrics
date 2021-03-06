# app.py
''' Get, record, and measure growth of Lando metrics. '''

from datetime import datetime
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers, exceptions
import os
import pandas as pd
from tabulate import tabulate

load_dotenv()
QBOX_USER = os.getenv('QBOX_USER')
QBOX_PASSWORD = os.getenv('QBOX_PASSWORD')

es = Elasticsearch([
        f'https://{QBOX_USER}:{QBOX_PASSWORD}@f505e785.qb0x.com:31626/'
    ])


def month_and_year(month):
    pieces = month.split(' ')
    return f'{pieces[1]} {pieces[2]}'


def get_metric_count(data, group=[]):
    if len(group) != 0 and len(data) != 0:
        metric = data.groupby(group).count()
    else:
        metric = data

    return len(metric)


def compute_growth(num_this_period, num_last_period):
    return str(
                round(
                    ((num_this_period - num_last_period) / num_last_period) * 100
                )
            ) + '%'


def get_recipe_metrics(start, end, recipe='lagoon'):
    ''' Get recipe metrics between start time and end time. '''
    start = int(datetime.strptime(start, '%d %B %Y').timestamp())
    end = int(datetime.strptime(end, '%d %B %Y').timestamp())
    doc = {
      'size': 10000,
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
    elif recipe == 'allapps':
        del doc['query']['filtered']['query']

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
    # ['01 April 2020', '30 April 2020'],
    # ['01 May 2020', '31 May 2020'],
    # ['01 June 2020', '30 June 2020'],
    # ['01 July 2020', '31 July 2020'],
    # ['01 August 2020', '31 August 2020'],
    # ['01 September 2020', '30 September 2020'],
    # ['01 October 2020', '31 October 2020'],
    # ['01 November 2020', '30 November 2020'],
    # ['01 December 2020', '31 December 2020'],
    # ['01 January 2021', '31 January 2021'],
    # ['01 February 2021', '28 February 2021'],
    ['01 March 2021', '31 March 2021'],
    ['01 April 2021', '30 April 2021'],
]
types = [
        # 'allapps',
        'acquia',
        # 'backdrop',
        # 'custom',
        # 'drupal6',
        # 'drupal7',
        # 'drupal8',
        # 'drupal9',
        # 'joomla',
        'lagoon',
        # 'laravel',
        # 'lamp',
        # 'lemp',
        'localdev',
        # 'mean',
        'pantheon',
        'platformsh',
        # 'symfony',
        # 'wordpress',
    ]

for type in types:
    last_month_uniq_apps = 0
    percent_growth_uniq_apps = 0
    last_month_uniq_users = 0
    percent_growth_uniq_users = 0
    last_month_commands = 0
    percent_growth_commands = 0
    data = []
    print(f'{type}:')
    header = [
                'Month',
                'Unique Users',
                'Growth',
                'Unique Apps',
                'Growth',
                'Num Commands',
                'Growth'
            ]
    for month in months:
        metric = get_recipe_metrics(month[0], month[1], type.lower())
        commands = get_metric_count(metric)
        uniq_apps = get_metric_count(metric, ['_source.app'])
        uniq_users = get_metric_count(metric, ['_source.instance'])
        if last_month_uniq_apps > 0:
            uniq_app_pg = compute_growth(uniq_apps, last_month_uniq_apps)
            uniq_users_pg = compute_growth(uniq_users, last_month_uniq_users)
            commands_pg = compute_growth(commands, last_month_commands)
        else:
            uniq_app_pg = 'NA'
            uniq_users_pg = 'NA'
            commands_pg = 'NA'

        last_month_uniq_apps = uniq_apps
        last_month_uniq_users = uniq_users
        last_month_commands = commands
        data.append([
                        month_and_year(month[0]),
                        uniq_users,
                        uniq_users_pg,
                        uniq_apps,
                        uniq_app_pg,
                        commands,
                        commands_pg
                    ])
    print(tabulate(data, header))
    print('\n')
