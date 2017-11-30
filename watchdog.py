#!/usr/bin/env python3
import argparse
import subprocess
import time
import threading
import logging

import daemon
from daemon import pidfile
import boto3
from botocore.exceptions import EndpointConnectionError

# constants
TABLE_NAME = 'aszymanski-watchdog-table'
LOG_FILE = '/var/log/aws-watchdog.log'
PID_FILE = '/var/run/aws-watchdog.pid'


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


def run_daemon(fetcher: ConfigFetcher):
    logger = logging.getLogger('aws-watchdog')

    logger.setLevel(logging.INFO)

    formatstring = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(formatstring)

    handler = logging.FileHandler(LOG_FILE)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    running_threads = dict()
    while True:
        config = fetcher.get_config()

        iteration_time = config['numOfSecCheck']
        retries = int(config['numOfAttempts'])
        retry_wait_time = int(config['numOfSecWait'])
        services = config['listOfServices']

        # update running threads
        for name in [n for n, t in running_threads.items() if not t.is_alive()]:
            del running_threads[name]

        for service_name in services:
            if (not run_service_command(service_name, 'status')
                and service_name not in running_threads):
                # service down and not being restarted
                logger.info(f'{service_name} is down')
                
                child = threading.Thread(target=restart_service_with_retries,
                                         args=(service_name,
                                               retries,
                                               retry_wait_time))
                child.start()
                running_threads[service_name] = child
            elif service_name in running_threads:
                logger.info(f'{service_name} is being restarted')
            else:
                logger.info(f'{service_name} is running')

        time.sleep(iteration_time)


def restart_service_with_retries(service_name: str,
                                 retries: int,
                                 wait_time: int):
    logger = logging.getLogger('aws-watchdog')
    for tries in range(1, retries+1):
        if run_service_command(service_name, 'restart'):
            logger.info(f'{service_name} restarted after {tries} times')
            return
        elif tries != retries:  # try again
            time.sleep(wait_time)
        else:
            logger.info(f'{service_name} failed after {retries} restarts')


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


def check_for_config(fetcher: ConfigFetcher):
    """Tries to download the configuration with given settings.
    Exits when cannot fetch the config or no rows are retruned.
    """
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


def main():
    # parse arguments
    description = 'Watchdog for checking services statuses'
    id_help = 'Id of the DynamoDB row with required configuration'
    parser = argparse.ArgumentParser(prog='watchdog', description=description)
    parser.add_argument('id', help=id_help)
    args = parser.parse_args()
  
    # initiate logging 
    fetcher = ConfigFetcher(args.id, TABLE_NAME)

    # exits if config is invalid
    # this would not work like that if started for example from init.d
    check_for_config(fetcher)

    # this part should be ran as a daemon
    with daemon.DaemonContext(pidfile=pidfile.TimeoutPIDLockFile(PID_FILE)) as context:
        run_daemon(fetcher)

if __name__ == '__main__':
    main()

