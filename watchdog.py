#!/usr/bin/env python3
import argparse

import boto3
from botocore.exceptions import EndpointConnectionError

# constants
TABLE_NAME = 'aszymanski-watchdog-table'

DESCRIPTION = 'Watchdog for checking services statuses'
ID_HELP = 'Id of the DynamoDB row with required configuration'


def main():
    parser = argparse.ArgumentParser(prog='watchdog', description=DESCRIPTION)
    parser.add_argument('id', help=ID_HELP)
    args = parser.parse_args()
    
    try:
        get_config(args.id)
    except KeyError:  # the configuration entry doesn't exist
        pass


def get_config(id: int):
    """Requests a row from db with the required config."""
    ddb = boto3.resource('dynamodb')
    table = ddb.Table(TABLE_NAME)

    try:
        res = table.get_item(Key={'id': id})
    except EndpointConnectionError:  # cannot connect to the DB
        pass


if __name__ == '__main__':
    main()

