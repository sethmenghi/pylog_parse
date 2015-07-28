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
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
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
        if os.path.isfile(current_path):
            if f[0] != '.' and os.path.getsize(current_path) != 0:
                yield current_path


class LogFile(object):

    """Parses a logfile into a pandas dataframe."""

    df = None
    _apache_headers = ['server', 'ip', 'rfc1413', 'userid', 'timestamp',
                       'request', 'status', 'size', 'referer', 'useragent',
                       'requestid']
    _apache_types = ['text', 'cidr', 'text', 'text', 'numeric',
                     'numeric', 'text', 'text', 'text']
    _apache_regex = re.compile((r'^(\S+) (\S+) (\S+) (\S+) (\S+) ' +
                                r'("?:\\"|.*?") (\S+) (\S+) ' +
                                r'("?:\\"|.*?") ("?:\\"|.*?") (\S+)'))

    def __init__(self, path, log_type='apache', date_cols=None):
        """Initialize the LogFile object.

        Args:
            path (str): path of LogFile
            log_type (str): type of log
                example: 'apache'
        """
        self.path = path
        self.file_ext = os.path.splitext(path)[1]
        self.filename = os.path.basename(os.path.splitext(path)[0])
        self.date_cols = date_cols
        self.log_type = log_type

    def _load_apache_log(self, path, headers=None):
        logger.info('Loading Apache Log - {path}'.format(path=path))
        data = []
        for i, line in enumerate(open(path)):
            for match in re.finditer(self._apache_regex, line):
                row = []
                for i, group in enumerate(match.groups()):
                    row.append(group)
                data.append(row)
        df = pd.DataFrame(data, index=range(0, len(data)))
        df.columns = self._apache_headers
        return df

    def _load_path(self, path):
        logger.info('Loading Path - {path}'.format(path=path))
        dfs = []
        file_ext = os.path.splitext(path)[1]
        df = pd.DataFrame()
        # unzip file and load contents
        if file_ext == '.zip' or 'gz' in file_ext:
            extracted_path = unzip(path, file_ext)
            for f in list_directory_files(extracted_path):
                df = self._load_path(f)
                dfs.append(df)
        # Recursively hit all files in directory
        elif file_ext == '':
            for f in list_directory_files(path):
                df = self._load_path(f)
                dfs.append(df)
        # Hit the log file! Process it.
        elif file_ext == '.log':
            if self.log_type == 'apache':
                df = self._load_apache_log(path)
        # Concat dataframes from zipfiles or directories
        if len(dfs) > 0:
            df = pd.concat(dfs, axis=0, ignore_index=True)
        return df

    def upload(self, engine, table=None, headers=None):
        """Upload the logfile to the table.

        Args:
            engine (sqlalchemy engine): database connection
            table (str): tablename to append to
            headers (list): headers of file
                if not supplied will default to file headers,
                if no file headers will check database tablename
        """
        if self.df is None:
            self.df = self._load_path(self.path)
        if table is None:
            table = self.filename
        logger.info('Uploading: {t} - {l}'.format(t=table, l=len(self.df)))
        self.df.to_sql(table, con=engine, if_exists='append', index=False)

    def to_csv(self, output):
        """Output apache log to file as csv."""
        if self.df is None:
            self.df = self._load_path(self.path)
        self.df.to_csv(output, index=False)
