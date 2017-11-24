#!/usr/bin/env python

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Loads a single row of data directly into BigQuery.
For more information, see the README.rst.
Example invocation:
    $ python stream_data.py example_dataset example_table \\
        '["Gandalf", 2000]'
The dataset and table should already exist.
"""

import argparse
import json
from pprint import pprint

from google.cloud import bigquery


def stream_data(dataset_id, table_id, rows):
    bigquery_client = bigquery.Client("pandora-154702")
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Get the table from the API so that the schema is available.
    table = bigquery_client.get_table(table_ref)

    errors = bigquery_client.create_rows(table, rows)

    if not errors:
        print('Loaded 1 row into {}:{}'.format(dataset_id, table_id))
    else:
        print('Errors:')
        pprint(errors)


if __name__ == '__main__':
    import datetime
    rows = [
        {
            "timesatamp": datetime.datetime.now(),
            "mid_price": 123456.2,
            "bids": [{"price":30.12, "size":10002.31}, {"price":30.15, "size":20002.31}],
            "asks": []
        },
        {
            "timesatamp": datetime.datetime.now(),
            "mid_price": 123456.2,
            "bids": [{"price":30.12, "size":10002.31}, {"price":30.15, "size":20002.31}],
            "asks": [{"price":0.92, "size":3130.31}, {"price":40.15, "size":4905.55},{"price":1, "size":0}]
        }
    ]

    stream_data(
        "trading",
        "test_board$20171125",
        rows)
