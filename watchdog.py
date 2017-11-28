#!/usr/bin/env python3
import argparse
import subprocess
import time

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
   
    fetcher = ConfigFetcher(args.id, TABLE_NAME)
    
    config = fetcher.get_config() 
    
    for service in config['listOfServices']:
        print(is_service_running(service), service)


def is_service_running(name: str):
    # This assumes the services are running under systemctl
    res = subprocess.run(['systemctl', 'status', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return True if res.returncode == 0 else False


def restart_service(name: str):
    # This assumes the services are running under systemctl
    try:
        res = subprocess.run(['systemctl', 'restart', name],
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        return True if res.returncode == 0 else False
    except SubprocessError:
        return False


class ConfigFetcher:
    def __init__(self, id: str, table_name: str):
        """Returns up to date config, fetching from DB when older than 15min.
        
        Params:
            id(str): id of the db row with requested config
            table_name(str): name of the table with requested config
        """
        ddb = boto3.resource('dynamodb')
        self._id = id
        self._table = ddb.Table(table_name)
        self._lastUpdated = None
        self._config = None

    def _fetch_config(self):
        response = self._table.get_item(Key={'id': self._id})
        self._config = response['Item']
        self._lastUpdated = time.time()
        
    def get_config(self) -> dict:
        """Requests a row from db with the required config.
        Raises:
             KeyError: when the config row doesn't exist
             EndpointConnectionError: when there is no connection to the database
        """
        if (self._lastUpdated is None
            or time.time() - self._lastUpdated > 900):
            # The config is older than 900sec/15minutes or new at all
            self._fetch_config()
        return self._config


if __name__ == '__main__':
    main()

