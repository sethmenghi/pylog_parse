===============================
pylog_parse
===============================

.. image:: https://img.shields.io/travis/sethmenghi/pylog_parse.svg
        :target: https://travis-ci.org/sethmenghi/pylog_parse

.. image:: https://img.shields.io/pypi/v/pylog_parse.svg
        :target: https://pypi.python.org/pypi/pylog_parse


Parses logs

* Free software: BSD license
* Documentation: https://pylog_parse.readthedocs.org.

Installation
------------

* Clone the github repository
* Install by going into directory in terminal and executing:

.. code-block:: bash

    $ python setup.py install


Example
--------

* To execute you may copy this script and input your own information
* Eventually command line arguments will be put in the place of manually
  creating a script


.. code-block:: python

    # filename: example.py

    import sqlalchemy
    from pylog_parse import LogFile

    # Path to zip file, doesn't have to be zip file
    log_zip = '/path/to/target/Logs.zip'

    # Create LogFile object
    log = LogFile(test)

    # Save log dataframe as csv
    output_csv = '/path/to/some/dest_folder/Logs.csv'
    log.to_csv(output_csv)

    # sqlalchemy connection
    conn_url = 'postgresql://scott:tiger@localhost:5432/database'
    engine = sqlalchemy.create_engine(conn_url)
    log.upload(engine, table='target_table')
