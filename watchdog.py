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
    # First run check if the config is correct and die if it's not.
    config = fetcher.get_config()

    while True:
        config = fetcher.get_config() 

        for service in config['listOfServices']:
          print(run_service_command(service, 'status'), service)
        
        time.sleep(config['numOfSecCheck'])


def run_service_command(service_name: str, cmd: str):
    """Run a shell command to check services.
    Returns:
        True: if the command succeded (0 return code)
        False: if the command didn't succeed or the shell call broke.
    """
    # This assumes the services are running under systemctl
    try:
        res = subprocess.run(['systemctl', cmd, service_name],
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        return True if res.returncode == 0 else False
    except subprocess.SubprocessError:
        return False


class ConfigFetcher:
    def __init__(self, id: str, table_name: str):
        """Returns up to date config,
         fetching from DB when older than 15min (when get_config is called).
        
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

