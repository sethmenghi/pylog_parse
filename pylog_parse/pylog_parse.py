# -*- coding: utf-8 -*-
"""Parses a log file and uploads it to a server."""
import os
import re
import errno
import zipfile
import tarfile
import logging

import pandas as pd


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
        logger.info('Loading Apache Log - {path}'.format(path=path))
        for i, line in enumerate(open(path)):
            for match in re.finditer(self.regex, line):
                row = [g for g in match.groups()]
                data.append(row)
        df = pd.DataFrame(data, index=range(0, len(data)))
        df.columns = self.headers
        return df

    def _load_directory(self, path, engine=None, table=None):
        dfs = []
        df = pd.DataFrame()
        for f in list_directory_files(path):
            df = self._load_path(f)
            dfs.append(df)
        df = pd.concat(dfs, axis=0, ignore_index=True)
        return df

    def _load_path(self, path, engine=None, table=None):
        logger.debug('Loading Path - {path}'.format(path=path))
        file_ext = os.path.splitext(path)[1]
        # unzip file and load contents
        if file_ext == '.zip' or 'gz' in file_ext:
            extracted_path = unzip(path, file_ext)
            df = self._load_directory(extracted_path)
        # Recursively hit all files in directory
        elif file_ext == '':
            df = self._load_directory(path)
        # Hit the log file! Process it.
        elif file_ext == '.log':
            df = self._load_log(path)
        else:
            df = pd.DataFrame()
        return df

    def to_csv(self, path):
        """Output apache log to file as csv."""
        if self.df is None:
            self.df = self._load_path(self.path)
        if os.path.isdir(path):
            path = path[0:-1] + '.csv'
        self.df.to_csv(path, index=False)

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
