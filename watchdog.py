#!/usr/bin/env python3
import argparse
import subprocess
import time
import threading

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
    try:
        config = fetcher.get_config()
    except KeyError:
        print('such a configuration doesn\'t exist in the database')
        exit(1)
    except EndpointConnectionError as err:
        print('no connection' + err.msg)
        exit(1)
    except:
        print('some other kind of error')
        exit(1)

    # this part should be ran as a daemon
    while True:   
        config = fetcher.get_config()

        retries = int(config['numOfAttempts'])
        wait_time = int(config['numOfSecWait'])
        service_statuses = dict.fromkeys(config['listOfServices'], False)
        
        for service_name in service_statuses.keys():
            if (not run_service_command(service_name, 'status')
                and service_statuses[service_name] == False):
                # service down and not being restarted
                print(f'{service_name} is down')
                
                child = threading.Thread(target=restart_service_with_retries,
                                         args=(service_name,
                                               service_statuses,
                                               retries,
                                               wait_time))
                child.start() 
            else:
                print(f'{service_name} is running') 
        
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


def restart_service_with_retries(service_name: str, 
                                 service_statuses: dict,
                                 retries: int,
                                 wait_time: int):
    service_statuses[service_name] = True  # service is being restarted
    for tries in range(retries):
        if run_service_command(service_name, 'restart'):
            print(f'the retry for {service_name} succeded after {tries+1} times')
            service_statuses[service_name] = False
            return
        elif tries != retries:  # try again
            time.sleep(wait_time) 
        else:
            print(f'could not restart {service_name} after {retries} times')
            service_statuses[service_name] = False


class ConfigFetcher:
    def __init__(self, id: str, table_name: str):
        """Returns up to date config, lazily
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

