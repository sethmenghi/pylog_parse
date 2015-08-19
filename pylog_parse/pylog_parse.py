# -*- coding: utf-8 -*-
"""Parses a log file and uploads it to a server."""
import os
import re
import errno
import zipfile
import tarfile
import logging
import traceback
import csv

import pandas as pd
import datetime


logger = logging.Logger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def mkdir(path):
    """Create directories & subdirectories similary to mkdir -p."""
    try:
        os.mkdir(path)
    except os.error, e:
        if e.errno != errno.EEXIST:
            raise


def unzip(path, file_ext):
    """Unzip a file and all contents to same directory.

    The folder is the same name as supplied path.
    """
    extracted_path = os.path.dirname(os.path.abspath(path))
    filename = os.path.basename(os.path.splitext(path)[0])
    logger.info('Extracting {path} -> {out}'.format(path=path,
                                                    out=extracted_path))
    if file_ext == '.zip':
        compressed_file = zipfile.ZipFile(path)
    elif 'gz' in file_ext:
        compressed_file = tarfile.TarFile(path)
    compressed_file.extractall(path=extracted_path)
    extracted_path = os.path.join(extracted_path, filename)
    return extracted_path


def list_directory_files(path):
    """Yield all filenames in a path."""
    for f in os.listdir(path):
        current_path = os.path.join(path, f)
        if f[0] != '.' and os.path.getsize(current_path) != 0:
            yield current_path


class LogFile(object):

    """Parses a logfile into a pandas dataframe."""

    df = None
    length = 0
    path_length = 0
    regex = None
    headers = None
    types = None
    _apache_headers = ['server', 'ip', 'rfc1413', 'userid', 'timestamp',
                       'request', 'status', 'size', 'referer', 'useragent',
                       'requestid']
    _apache_types = ['text', 'cidr', 'text', 'text', 'numeric',
                     'numeric', 'text', 'text', 'text']
    _apache_regex = re.compile((r'^(\S+) (\S+) (\S+) (\S+) (\S+) ' +
                                r'("?:\\"|.*?") (\S+) (\S+) ' +
                                r'("?:\\"|.*?") ("?:\\"|.*?") (\S+)'))
    _elblogs_types = {'timestamp': 'timestamp', 'text': 'elb',
                      'cidr': 'clientip', 'cidr': 'backendip',
                      'time': 'request_processing_time',
                      'time': 'backend_processing_time',
                      'time': 'response_processing_time',
                      'numeric': 'elb_status_code',
                      'numeric': 'backend_status_code',
                      'numeric': 'received_bytes', 'numeric': 'sent_bytes',
                      'text': 'request', 'text': 'user_agent'}
    _elblogs_headers = ['timestamp', 'elb', 'clientip', 'backendip',
                        'request_processing_time', 'backend_processing_time',
                        'response_processing_time', 'elb_status_code',
                        'backend_status_code', 'received_bytes', 'sent_bytes',
                        'request', 'user_agent']
    _elblogs_regex = re.compile((r'^(\S+) (\S+) (\S+):\S+ (\S+):\S+ (\S+) ' +
                                 r'(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ' +
                                 r'("?.*?") ?(?:(.*))'))

    def __init__(self, path, log_type):
        """Initialize the LogFile object.

        Args:
            path (str): path of LogFile
            log_type (str): type of log
                example: 'apache'
        """
        self.path = path
        self.file_ext = os.path.splitext(path)[1]
        self.filename = os.path.basename(os.path.splitext(path)[0])
        self.log_type = log_type
        self._assign_regex()
        # self.df = self._load_path(path)
        # self.encoding = encoding

    def _assign_regex(self):
        if self.log_type == 'apache':
            self.regex = self._apache_regex
            self.headers = self._apache_headers
            self.types = self._apache_types
        elif self.log_type == 'elblogs':
            self.regex = self._elblogs_regex
            self.headers = self._elblogs_headers
            self.types = self._elblogs_types

    @property
    def data(self):
        """Return processed dataframe of LogFile."""
        if self.df is None:
            self.df = self._load_path(self.path)
        return self.df

    def _load_log(self, path, engine=None, table=None):
        data = []
        # logger.debug('Loading Apache Log - {path}'.format(path=path))
        for i, line in enumerate(open(path)):
            for match in re.finditer(self.regex, line):
                row = [g for g in match.groups()]
                data.append(row)
        df = pd.DataFrame(data, index=range(0, len(data)))
        logger.info('Length: {l} \t {path}'.format(l=len(df), path=path))
        self.length += len(df)
        df.columns = self.headers
        return df

    def _load_directory(self, path, engine=None, table=None, csv_path=None):
        dfs = []
        for f in list_directory_files(path):
            df = self._load_path(f)
            if csv_path:
                if os.path.exists(csv_path):
                    with open(csv_path, 'a') as f:
                        df.to_csv(csv_path, sep=',',
                                  quoting=csv.QUOTE_NONNUMERIC,
                                  na_rep='NULL', index=False)
                else:
                    df.to_csv(csv_path, mode='wb', sep=',',
                              quoting=csv.QUOTE_NONNUMERIC,
                              na_rep='NULL', index=False)
            else:
                dfs.append(df)
        if len(dfs) > 0:
            df = pd.concat(dfs, axis=0, ignore_index=True)
        else:
            df = None
        return df

    def _load_path(self, path, engine=None, table=None, csv_path=None):
        # logger.debug('Loading Path - {path}'.format(path=path))
        file_ext = os.path.splitext(path)[1]
        # unzip file and load contents
        if file_ext == '.zip' or 'gz' in file_ext:
            extracted_path = unzip(path, file_ext)
            df = self._load_directory(extracted_path)
        # Recursively hit all files in directory
        elif file_ext == '':
            df = self._load_directory(path, csv_path=csv_path)
        # Hit the log file! Process it.
        elif file_ext == '.log':
            df = self._load_log(path)
            self.path_length += len(df)
        else:
            df = pd.DataFrame()
        return df

    def to_csv(self, path, copy=False, con=None):
        """Output apache log to file as csv."""
        if self.df is None:
            self.df = self._load_path(self.path, csv_path=path)
        logger.info('TotalLength: {l}, PathLength {pl} \t {path}'.format(l=self.length, pl=self.path_length, path=path))
        #  self.df.to_csv(path, mode='wb', sep=',', quoting=csv.QUOTE_NONNUMERIC, na_rep='NULL', index=False)
        if copy:
            if not con:
                raise Exception('Need psycopg2 connection')
            try:
                conn = con
                cur = conn.cursor()
                copy_sql = """
                    COPY elblogs FROM stdin WITH
                    NULL AS 'NULL'
                    CSV HEADER
                    DELIMITER as ','
                 """
                with open(path, 'r') as f:
                    cur.execute("SET CLIENT_ENCODING TO 'latin1'")
                    logger.info('COPY {p} -> db'.format(p=path))
                    cur.copy_expert(sql=copy_sql, file=f)
                    conn.commit()
                    cur.close()
                del self.df
            except Exception, e:
                del self.df
                if cur:
                    cur.close()
                logger.info(traceback.format_exc())
                logger.info(e)
                if os.path.exists(path):
                    logger.info('Removing {p}'.format(p=path))
                    os.remove(path)

    def to_sql(self, engine, table=None, headers=None):
        """Upload the logfile to the table.

        Args:
            engine (sqlalchemy engine): database connection
            table (str): tablename to append to
            headers (list): headers of file
                if not supplied will default to file headers,
                if no file headers will check database tablename
        """
        if self.df is None:
            self.df = self._load_path(self.path, engine, table)
        if table is None:
            if self.log_type == 'apache':
                table = 'apachelogs'
            elif self.log_type == 'elblogs':
                table = 'elblogs'
            table = self.filename
        logger.info('Uploading: {t} - {l}'.format(t=table, l=len(self.df)))
        try:
            engine.execute("SET client_encoding TO 'latin_1'")
        except Exception, e:
            logger.info(e)
            pass
        try:
            logger.info("Upload: {f} -> {t}".format(f=self.filename, t=table))
            self.df.to_sql(table, con=engine, if_exists='append',
                           index=False, chunksize=1000)
        except Exception, e:
            logger.info(e)
            raise(e)
