#!/usr/bin/env python3
import argparse
import subprocess
import time
import threading
import logging
from logging.handlers import TimedRotatingFileHandler

import daemon
from daemon import pidfile
import boto3
from botocore.exceptions import EndpointConnectionError

# constants
PROG_NAME = 'aws-watchdog'
LOGGER_NAME = f'{PROG_NAME}.daemon'

TABLE_NAME = 'aszymanski-watchdog-table'
SNS_TOPIC = 'arn:aws:sns:us-west-2:632826021673:aszymanski-watchdog-topic'
S3_BUCKET_NAME = 'aszymanski-watchdog-s3'

LOG_FILE = '/var/log/aws-watchdog/watchdog.log'
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
             EndpointConnectionError: when connection to the db is down
        """
        if (self._lastUpdated is None
            or time.time() - self._lastUpdated > 900):
            # The config is older than 900sec/15minutes or new at all
            self._fetch_config()
        return self._config


class RotatingTimedS3FileHandler(TimedRotatingFileHandler):
    """A rorating timed log handler which also uploads the logs to S3"""
    def doRollover(self):
        # upload the file to s3 then continue logging
        
        name = time.strftime(self.suffix, 
                             time.gmtime(self.rolloverAt - self.interval))

        s3 = boto3.client('s3')
        s3.upload_file(self.baseFilename, S3_BUCKET_NAME, name + '.log')

        super().doRollover() 


class SNSHandler(logging.Handler):
    """A logging handler which sends messages to SNS"""
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self._client = boto3.client('sns')
        self._logger = logging.getLogger(f'{PROG_NAME}.SNSHandler')

    def emit(self, record: logging.LogRecord):
        msg = f'{record.created}[{record.levelname}]:{record.msg}'
        try:
            self._client.publish(TopicArn=SNS_TOPIC,
                                 Message=record.msg)
        except EndpointConnectionError:
            self._logger.error('Error when publishing message to sns')


def run_daemon(fetcher: ConfigFetcher):
    init_loggers()
    logger = logging.getLogger(LOGGER_NAME)

    running_threads = dict()
    while True:
        try:
            config = fetcher.get_config()
        except EndpointConnectionError: 
            logger.warn('Can\'t update configuration: dynamodb unavailable')

        iteration_time = 10  # config['numOfSecCheck']
        retries = int(config['numOfAttempts'])
        retry_wait_time = int(config['numOfSecWait'])
        services = config['listOfServices']

        # update running threads
        for name in [n for n, t
                     in running_threads.items()
                     if not t.is_alive()]:
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

        time.sleep(iteration_time)


def init_loggers():
    # Root logger logs only to file
    logger = logging.getLogger(PROG_NAME)
    logger.setLevel(logging.INFO)

    formatstring = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(formatstring)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    # This handler will push logs to s3 every hour
    file_handler = RotatingTimedS3FileHandler(LOG_FILE,
                                              when="h",
                                              interval=1,
                                              backupCount=5)

    file_handler.setFormatter(formatter)

    sns_handler = SNSHandler(level=logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # This logger logs to both file/sns
    program_logger = logging.getLogger(LOGGER_NAME)
    program_logger.addHandler(sns_handler)


def restart_service_with_retries(service_name: str,
                                 retries: int,
                                 wait_time: int):
    logger = logging.getLogger(LOGGER_NAME)
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
        print('Can\'t start daemon: wrong configuration')
        exit(1)
    except EndpointConnectionError as err:
        print('Cannot connect to the DynamoDB instance, terminating')
        exit(1)
    except Exception:
        print('Unexpected error happened')
        exit(1)


def main():
    # parse arguments
    description = 'Watchdog for checking services statuses'
    id_help = 'Id of the DynamoDB row with required configuration'
    parser = argparse.ArgumentParser(prog=PROG_NAME, description=description)
    parser.add_argument('id', help=id_help)
    args = parser.parse_args()

    # initiate logging
    fetcher = ConfigFetcher(args.id, TABLE_NAME)

    # exits if config is invalid
    # this would not work like that if started for example from init.d
    check_for_config(fetcher)

    # this part should be ran as a daemon
    pidf = pidfile.TimeoutPIDLockFile(PID_FILE)
    with daemon.DaemonContext(pidfile=pidf) as context:
        run_daemon(fetcher)


if __name__ == '__main__':
    main()
