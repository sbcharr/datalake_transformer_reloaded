import logging as log
import configparser
import argparse
import os
import sys
from datetime import datetime, timedelta


__config = configparser.RawConfigParser()
__config.read(os.path.join(os.getcwd(), 'dl.cfg'))

# os.environ['AWS_ACCESS_KEY_ID'] = __config['aws-creds']['aws_access_key_id']
# os.environ['AWS_SECRET_ACCESS_KEY'] = __config['aws-creds']['aws_secret_access_key']
# os.environ['REGION_NAME'] = __config['aws-creds']['region_name']
#
# os.environ['SOURCE_BUCKET_NAME'] = __config['aws-s3']['source_bucket_name']
# os.environ['DESTINATION_BUCKET_NAME'] = __config['aws-s3']['destination_bucket_name']
#
# os.environ['TABLES'] = __config['table-info']['tables']
#
# os.environ['SOURCE_PREFIX_DATE_FORMAT'] = __config['table-info']['source_prefix_date_format']
# os.environ['DESTINATION_PREFIX_DATE_FORMAT'] = __config['table-info']['destination_prefix_date_format']

AWS_ACCESS_KEY_ID = __config['aws-creds']['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = __config['aws-creds']['aws_secret_access_key']
REGION_NAME = __config['aws-creds']['region_name']
SOURCE_BUCKET_NAME = __config['aws-s3']['source_bucket_name']
DESTINATION_BUCKET_NAME = __config['aws-s3']['destination_bucket_name']
TABLES = __config['table-info']['tables']
SOURCE_PREFIX_DATE_FORMAT = __config['table-info']['source_prefix_date_format']
# DESTINATION_PREFIX_DATE_FORMAT = __config['table-info']['destination_prefix_date_format']
RUN_HOUR = "00"


def flag_parser():
    """
    A function to parse parameterized input to command line arguments. It returns the object
    containing values of each input argument in a key/value fashion.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobName", help="(optional for admin) job name to execute")
    parser.add_argument("--jobInstance", help="sequence number of job instance")
    parser.add_argument("--userType", help="user who runs this job, one of 'admin' or 'user'")
    parser.add_argument("--maxDpu", help="(optional) max dpu that AWS Glue uses, available only with user type 'user'")
    parser.add_argument("--logLevel", help="(optional) log level, values are 'debug', 'info', \
                                           'warning', 'error', 'critical'")
    parser.add_argument("--batchSize", help="(optional for admin) number of tables to process")

    args = parser.parse_args()

    return args


def set_logger(log_level=log.INFO):
    """
    Main logger set for the program. Default log level is set to INFO.
    """
    log_level_switcher = {
        'debug': log.DEBUG,
        'info': log.INFO,
        'warning': log.WARNING,
        'error': log.ERROR,
        'critical': log.CRITICAL
    }
    log.basicConfig(level=log_level_switcher.get(log_level, log.INFO))


def setup():
    # args = flag_parser()
    set_logger()

    # return args


def get_list_of_tables(table=None):
    tables = TABLES.strip().split('|')

    if table is not None:
        t = []
        if table in tables:
            t.append(table)
            return t
        else:
            log.error("Not a valid table - '{}', existing...".format(table))
            sys.exit(1)

    return tables


def get_run_date(days_to_subtract=1):
    run_date = (datetime.today() - timedelta(days=days_to_subtract)).strftime(SOURCE_PREFIX_DATE_FORMAT)

    return run_date


def get_put_date(days_to_subtract=1):
    put_date = (datetime.today() - timedelta(days=days_to_subtract)).strftime(SOURCE_PREFIX_DATE_FORMAT + RUN_HOUR)

    return put_date

