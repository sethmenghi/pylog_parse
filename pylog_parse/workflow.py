"""Workflow for uploading many many many log files at once."""
from __future__ import absolute_import
import os
from os.path import isfile, getsize
import logging

import luigi
# import sqlalchemy

from .pylog_parse import LogFile

logger = logging.getLogger(__name__)

class LogTask(luigi.Task):

    """Base task for log workflow."""

    path = luigi.Parameter()
    logtype = luigi.Parameter()

    _conf = luigi.configuration.get_config()
    _conf.reload()

    _user = _conf.get('postgres', 'user')
    _password = _conf.get('postgres', 'password')
    _host = _conf.get('postgres', 'host')
    _port = _conf.get('postgres', 'port')
    _database = _conf.get('pylog', 'database')

    _url = """postgresql://{u}:{p}@{h}:{port}/{db}
           """.format(u=_user, p=_port, h=_host,
                      port=_port, db=_database)

    @property
    def url(self):
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
        csv = os.path.splitext(self.path)[0]
        if os.path.isdir(self.path):
            csv = self.path[0:-1] + '.csv'
        csv = csv + '.csv'
        return luigi.LocalTarget(path=csv)

    def run(self):
        log = LogFile(path=self.path, log_type=self.logtype)
        log.to_csv(self.output().path)


class LogPaths(LogTask):
    run = NotImplemented

    def complete(self):
        return False

    def requires(self):
        log_files = [f for f in get_sublogs(self.path)]
        subfolders = [f for f in get_subfolders(self.path)]
        logger.info('Path: {p}'.format(p=self.path))
        logger.info('Subfolders: {s}'.format(s=subfolders))
        logger.info('Subfiles: {f}'.format(f=log_files))

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
