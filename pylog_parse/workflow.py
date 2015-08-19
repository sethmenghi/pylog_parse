"""Workflow for uploading many many many log files at once."""
from __future__ import absolute_import
import os
from os.path import isfile, getsize
import logging
import re

import luigi
import psycopg2
import pandas as pd

# import sqlalchemy
try:
    from .pylog_parse import LogFile
except:
    from pylog_parse import LogFile

logger = logging.getLogger(__name__)


class LogTask(luigi.Task):

    """Base task for log workflow."""

    path = luigi.Parameter()
    logtype = luigi.Parameter()
    _conf = luigi.configuration.get_config()
    _conf.reload()

    _password = _conf.get('postgres', 'password')
    _host = _conf.get('postgres', 'host')
    _port = _conf.get('postgres', 'port')
    _database = _conf.get('pylog', 'database')
    _user = _conf.get('postgres', 'user')
    _url = """postgresql://{u}:{p}@{h}:{port}/{db}""".format(u=_user,
                                                             p=_password,
                                                             h=_host,
                                                             port=_port,
                                                             db=_database)

    @property
    def url(self):
        """Postgresql url property."""
        return self._url


def folder_size(path):
    """Return size of folder at path."""
    return sum(getsize(f) for f in os.listdir('.') if isfile(f))


def list_directory_files(path, folders=False):
    """Yield all filenames in a path."""
    for f in os.listdir(path):
        if f[0] == '.':
            continue
        current_path = os.path.join(path, f)
        if folders is False:
            if os.path.isfile(current_path):
                if os.path.getsize(current_path) != 0:
                    yield current_path
        else:
            file_ext = os.path.splitext(f)[1]
            if file_ext == '' or file_ext == '/':
                yield current_path


def get_subfolders(path):
    for f in list_directory_files(path, folders=True):
        file_ext = os.path.splitext(f)[1]
        if file_ext == ''or file_ext == '/':
            yield f


def get_sublogs(path):
    for f in list_directory_files(path):
        file_ext = os.path.splitext(f)[1]
        if file_ext == '.log':
            yield f


class CheckLogPath(luigi.ExternalTask):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(path=self.path)


class UploadLogs(LogTask):

    def requires(self):
        return CheckLogPath(path=self.path)

    def output(self):
        groups = re.search(r'2015.(\d{2}).(\d{2})', self.path).groups()
        csv = '/home/ubuntu/elb2/2015-{m}-{d}.csv'.format(m=groups[0],
                                                          d=groups[1])
        return luigi.LocalTarget(path=csv)

    def run(self):
        conn = psycopg2.connect(self.url)
        log = LogFile(path=self.path, log_type=self.logtype)
        cursor = conn.cursor()
        cursor.close()
        log.to_csv(self.output().path, con=conn, copy=True)
        if os.path.exists(self.output().path):
            df = pd.DataFrame({'length': log.length}, index=[0])
            os.remove(self.output().path)
            df.to_csv(self.output().path, index=False)  # Only keep head of csv


class LogPaths(LogTask):

    def requires(self):
        log_files = [f for f in get_sublogs(self.path)]
        subfolders = [f for f in get_subfolders(self.path)]
        logger.debug('Path: {p}'.format(p=self.path))
        logger.debug('Subfolders: {s}'.format(s=subfolders))
        logger.debug('Subfiles: {f}'.format(f=log_files))

        for fold in subfolders:
            sub = [f for f in get_subfolders(fold)]
            files = [f for f in get_sublogs(fold)]
            if len(sub) > 0:
                logger.info('Subfolders of {f}: {s}'.format(f=fold, s=sub))
                yield LogPaths(path=fold, logtype=self.logtype)
            elif len(files) > 0:
                yield UploadLogs(path=fold, logtype=self.logtype)
        for f in log_files:
            yield UploadLogs(path=f, logtype=self.logtype)

    def run(self):
        total_length = 0
        for f in self.input():
            total_length = total_length + pd.read_csv(f.path()).iloc[0][0]
        logger.info('AllFilesLength: {l}'.format(l=total_length))

if __name__ == '__main__':
    luigi.run()
